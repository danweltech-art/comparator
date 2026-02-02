"""Tests for ShadowDiff comparison engine."""

import pytest
from shadowdiff import ShadowDiffEngine, EngineConfig, DiffType


class TestBasicComparison:
    """Test basic comparison functionality."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_identical_payloads(self):
        """Test that identical payloads match."""
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        old = {"name": "test"}
        new = {"name": "test"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True
        assert len(result.diffs) == 0

    def test_different_values(self):
        """Test that different values are detected."""
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        old = {"name": "old"}
        new = {"name": "new"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False
        assert len(result.diffs) == 1
        assert result.diffs[0].type == DiffType.VALUE_MISMATCH

    def test_missing_field_in_new(self):
        """Test detection of missing field in new payload."""
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        old = {"name": "test"}
        new = {}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False
        assert any(d.type == DiffType.MISSING_IN_NEW for d in result.diffs)

    def test_extra_field_in_new(self):
        """Test detection of extra field in new payload."""
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        old = {"name": "test"}
        new = {"name": "test", "extra": "value"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False
        assert any(d.type == DiffType.EXTRA_IN_NEW for d in result.diffs)


class TestMigrationStrategy:
    """Test x-migration-strategy extension."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_strategy_ignore(self):
        """Test ignore strategy removes field from comparison."""
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "timestamp": {"type": "string", "x-migration-strategy": "ignore"}
            }
        }
        old = {"id": "1", "timestamp": "2025-01-01"}
        new = {"id": "1", "timestamp": "2025-02-02"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_strategy_exists(self):
        """Test exists strategy only checks presence."""
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string", "x-migration-strategy": "exists"}
            }
        }
        old = {"id": "old-value"}
        new = {"id": "completely-different-value"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_strategy_lenient(self):
        """Test lenient strategy ignores case and whitespace."""
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "x-migration-strategy": "lenient"}
            }
        }
        old = {"name": "  HELLO WORLD  "}
        new = {"name": "hello world"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True


class TestNumericComparison:
    """Test numeric comparison with precision."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_precision_within_tolerance(self):
        """Test values within precision tolerance match."""
        schema = {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "x-migration-precision": 0.01}
            }
        }
        old = {"amount": 100.00}
        new = {"amount": 100.005}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_precision_exceeds_tolerance(self):
        """Test values exceeding precision tolerance fail."""
        schema = {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "x-migration-precision": 0.01}
            }
        }
        old = {"amount": 100.00}
        new = {"amount": 100.05}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False
        assert result.diffs[0].type == DiffType.PRECISION_EXCEEDED


class TestStringComparison:
    """Test string comparison rules."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_case_insensitive(self):
        """Test case insensitive comparison."""
        schema = {
            "type": "object",
            "properties": {
                "status": {"type": "string", "x-migration-case-insensitive": True}
            }
        }
        old = {"status": "ACTIVE"}
        new = {"status": "active"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_trim_whitespace(self):
        """Test whitespace trimming."""
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "x-migration-trim-whitespace": True}
            }
        }
        old = {"name": "  hello  "}
        new = {"name": "hello"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_pattern_matching(self):
        """Test pattern validation."""
        schema = {
            "type": "object",
            "properties": {
                "code": {"type": "string", "x-migration-pattern": "^[A-Z]{3}-\\d{4}$"}
            }
        }
        old = {"code": "ABC-1234"}
        new = {"code": "XYZ-5678"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_pattern_mismatch(self):
        """Test pattern mismatch detection."""
        schema = {
            "type": "object",
            "properties": {
                "code": {"type": "string", "x-migration-pattern": "^[A-Z]{3}-\\d{4}$"}
            }
        }
        old = {"code": "ABC-1234"}
        new = {"code": "invalid"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False
        assert result.diffs[0].type == DiffType.PATTERN_MISMATCH


class TestAliasMapping:
    """Test x-migration-alias extension."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_field_alias(self):
        """Test field name aliasing."""
        schema = {
            "type": "object",
            "properties": {
                "customerId": {"type": "string", "x-migration-alias": "customer_id"}
            }
        }
        old = {"customer_id": "C123"}
        new = {"customerId": "C123"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True


class TestEnumMapping:
    """Test x-migration-enum-map extension."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_enum_mapping(self):
        """Test enum value mapping."""
        schema = {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "x-migration-enum-map": {
                        "ACTIVE": "active",
                        "INACTIVE": "inactive"
                    }
                }
            }
        }
        old = {"status": "ACTIVE"}
        new = {"status": "active"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True


class TestArrayComparison:
    """Test array comparison modes."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_strict_array_mode(self):
        """Test strict array comparison (order matters)."""
        schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "x-migration-array-mode": "strict",
                    "items": {"type": "string"}
                }
            }
        }
        old = {"items": ["a", "b", "c"]}
        new = {"items": ["a", "c", "b"]}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False

    def test_unordered_array_mode(self):
        """Test unordered array comparison."""
        schema = {
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "x-migration-array-mode": "unordered",
                    "items": {"type": "string"}
                }
            }
        }
        old = {"tags": ["a", "b", "c"]}
        new = {"tags": ["c", "a", "b"]}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_keyed_array_mode(self):
        """Test keyed array comparison."""
        schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "x-migration-array-mode": "keyed",
                    "x-migration-array-key": "id",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "value": {"type": "integer"}
                        }
                    }
                }
            }
        }
        old = {"items": [{"id": "a", "value": 1}, {"id": "b", "value": 2}]}
        new = {"items": [{"id": "b", "value": 2}, {"id": "a", "value": 1}]}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_ignore_extra_items(self):
        """Test ignore-extra-items flag."""
        schema = {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "x-migration-ignore-extra-items": True,
                    "items": {"type": "string"}
                }
            }
        }
        old = {"items": ["a", "b"]}
        new = {"items": ["a", "b", "c", "d"]}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True
        assert len(result.warnings) > 0


class TestGlobalRules:
    """Test global migration rules."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_global_ignores(self):
        """Test global ignore patterns."""
        schema = {
            "type": "object",
            "x-migration-global-ignores": ["$..timestamp", "$..metadata"],
            "properties": {
                "id": {"type": "string"},
                "timestamp": {"type": "string"},
                "metadata": {"type": "object"}
            }
        }
        old = {"id": "1", "timestamp": "old", "metadata": {"trace": "abc"}}
        new = {"id": "1", "timestamp": "new", "metadata": {"trace": "xyz"}}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_allow_null_as_missing(self):
        """Test null-as-missing behavior."""
        schema = {
            "type": "object",
            "x-migration-allow-null-as-missing": True,
            "properties": {
                "id": {"type": "string"},
                "optional": {"type": "string"}
            }
        }
        old = {"id": "1", "optional": None}
        new = {"id": "1"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True


class TestDatetimeComparison:
    """Test datetime comparison with tolerance."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_datetime_within_tolerance(self):
        """Test datetimes within tolerance match."""
        schema = {
            "type": "object",
            "properties": {
                "createdAt": {
                    "type": "string",
                    "x-migration-datetime-format": "ISO8601",
                    "x-migration-datetime-tolerance": "5s"
                }
            }
        }
        old = {"createdAt": "2025-02-02T10:30:00Z"}
        new = {"createdAt": "2025-02-02T10:30:03Z"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_datetime_exceeds_tolerance(self):
        """Test datetimes exceeding tolerance fail."""
        schema = {
            "type": "object",
            "properties": {
                "createdAt": {
                    "type": "string",
                    "x-migration-datetime-format": "ISO8601",
                    "x-migration-datetime-tolerance": "5s"
                }
            }
        }
        old = {"createdAt": "2025-02-02T10:30:00Z"}
        new = {"createdAt": "2025-02-02T10:30:10Z"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is False
        assert result.diffs[0].type == DiffType.DATETIME_EXCEEDED


class TestDefaultValues:
    """Test x-migration-default extension."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_default_injection(self):
        """Test default value injection."""
        schema = {
            "type": "object",
            "properties": {
                "quantity": {"type": "integer", "x-migration-default": 0}
            }
        }
        old = {}
        new = {"quantity": 0}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True


class TestTypeCasting:
    """Test x-migration-cast extension."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_cast_to_string(self):
        """Test casting to string."""
        schema = {
            "type": "object",
            "properties": {
                "value": {"type": "string", "x-migration-cast": "string"}
            }
        }
        old = {"value": 123}
        new = {"value": "123"}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True

    def test_cast_to_int(self):
        """Test casting to integer."""
        schema = {
            "type": "object",
            "properties": {
                "value": {"type": "integer", "x-migration-cast": "int"}
            }
        }
        old = {"value": "42"}
        new = {"value": 42}

        result = self.engine.compare(old, new, schema)
        assert result.is_match is True


class TestEngineConfig:
    """Test engine configuration options."""

    def test_fail_fast(self):
        """Test fail_fast stops on first error."""
        config = EngineConfig(fail_fast=True)
        engine = ShadowDiffEngine(config)

        schema = {
            "type": "object",
            "properties": {
                "a": {"type": "string"},
                "b": {"type": "string"},
                "c": {"type": "string"}
            }
        }
        old = {"a": "1", "b": "2", "c": "3"}
        new = {"a": "x", "b": "y", "c": "z"}

        result = engine.compare(old, new, schema)
        assert result.is_match is False
        assert len(result.diffs) == 1

    def test_trace_rule_application(self):
        """Test rule tracing."""
        config = EngineConfig(trace_rule_application=True)
        engine = ShadowDiffEngine(config)

        schema = {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "x-migration-precision": 0.01}
            }
        }
        old = {"amount": 100.00}
        new = {"amount": 100.005}

        result = engine.compare(old, new, schema)
        assert len(result.trace) > 0


class TestErrorHandling:
    """Test error handling."""

    def setup_method(self):
        self.engine = ShadowDiffEngine()

    def test_invalid_schema(self):
        """Test handling of invalid schema."""
        result = self.engine.compare({}, {}, "not a dict")
        assert result.success is False
        assert "VALIDATION_ERROR" in result.error["code"]

    def test_payload_size_limit(self):
        """Test payload size limit."""
        config = EngineConfig(max_payload_size_mb=0.0001)
        engine = ShadowDiffEngine(config)

        large_payload = {"data": "x" * 10000}
        schema = {"type": "object"}

        result = engine.compare(large_payload, large_payload, schema)
        assert result.success is False
        assert "PAYLOAD_SIZE_ERROR" in result.error["code"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

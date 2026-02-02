"""Data models for ShadowDiff engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
from datetime import datetime


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class DiffType(Enum):
    VALUE_MISMATCH = "VALUE_MISMATCH"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    MISSING_IN_NEW = "MISSING_IN_NEW"
    EXTRA_IN_NEW = "EXTRA_IN_NEW"
    ARRAY_LENGTH_MISMATCH = "ARRAY_LENGTH_MISMATCH"
    ARRAY_ITEM_MISSING = "ARRAY_ITEM_MISSING"
    ARRAY_ITEM_EXTRA = "ARRAY_ITEM_EXTRA"
    DUPLICATE_KEY = "DUPLICATE_KEY"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    PRECISION_EXCEEDED = "PRECISION_EXCEEDED"
    PATTERN_MISMATCH = "PATTERN_MISMATCH"
    DATETIME_EXCEEDED = "DATETIME_EXCEEDED"


class Severity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class MigrationStrategy(Enum):
    STRICT = "strict"
    IGNORE = "ignore"
    EXISTS = "exists"
    LENIENT = "lenient"


class ArrayMode(Enum):
    STRICT = "strict"
    UNORDERED = "unordered"
    KEYED = "keyed"


class DuplicateHandling(Enum):
    ERROR = "error"
    FIRST = "first"
    LAST = "last"
    MERGE = "merge"


class CastType(Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"


@dataclass
class EngineConfig:
    """Global configuration for the comparison engine."""
    max_depth: int = 100
    max_payload_size_mb: float = 50
    timeout_seconds: int = 30
    strict_schema_validation: bool = True
    collect_statistics: bool = True
    log_level: LogLevel = LogLevel.INFO
    trace_rule_application: bool = False
    fail_fast: bool = False


@dataclass
class DiffEntry:
    """A single difference found during comparison."""
    path: str
    type: DiffType
    severity: Severity
    old_value: Any
    new_value: Any
    message: str
    rule_applied: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "type": self.type.value,
            "severity": self.severity.value,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "message": self.message,
            "rule_applied": self.rule_applied,
        }


@dataclass
class WarningEntry:
    """A warning generated during comparison."""
    path: str
    type: DiffType
    severity: Severity
    message: str

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "type": self.type.value,
            "severity": self.severity.value,
            "message": self.message,
        }


@dataclass
class TraceEntry:
    """Trace entry for rule application (when trace_rule_application=true)."""
    path: str
    rule: str
    action: str
    details: Optional[dict] = None

    def to_dict(self) -> dict:
        result = {
            "path": self.path,
            "rule": self.rule,
            "action": self.action,
        }
        if self.details:
            result["details"] = self.details
        return result


@dataclass
class ExecutionInfo:
    """Execution metadata."""
    duration_ms: int
    timestamp: str
    engine_version: str = "2.0.0"

    def to_dict(self) -> dict:
        return {
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp,
            "engine_version": self.engine_version,
        }


@dataclass
class Summary:
    """Summary statistics of comparison."""
    total_fields_checked: int = 0
    mismatches_found: int = 0
    warnings_count: int = 0
    fields_ignored: int = 0

    def to_dict(self) -> dict:
        return {
            "total_fields_checked": self.total_fields_checked,
            "mismatches_found": self.mismatches_found,
            "warnings_count": self.warnings_count,
            "fields_ignored": self.fields_ignored,
        }


@dataclass
class Coverage:
    """Schema coverage information."""
    fields_in_schema: int = 0
    fields_in_payload: int = 0
    unmatched_in_old: list = field(default_factory=list)
    unmatched_in_new: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "fields_in_schema": self.fields_in_schema,
            "fields_in_payload": self.fields_in_payload,
            "unmatched_in_old": self.unmatched_in_old,
            "unmatched_in_new": self.unmatched_in_new,
        }


@dataclass
class DiffReport:
    """Complete comparison report."""
    is_match: bool
    execution: ExecutionInfo
    summary: Summary
    diffs: list[DiffEntry] = field(default_factory=list)
    warnings: list[WarningEntry] = field(default_factory=list)
    coverage: Optional[Coverage] = None
    trace: list[TraceEntry] = field(default_factory=list)

    def to_dict(self) -> dict:
        result = {
            "is_match": self.is_match,
            "execution": self.execution.to_dict(),
            "summary": self.summary.to_dict(),
            "diffs": [d.to_dict() for d in self.diffs],
            "warnings": [w.to_dict() for w in self.warnings],
        }
        if self.coverage:
            result["coverage"] = self.coverage.to_dict()
        if self.trace:
            result["trace"] = [t.to_dict() for t in self.trace]
        return result


@dataclass
class FieldRules:
    """Extracted migration rules for a field from schema."""
    strategy: MigrationStrategy = MigrationStrategy.STRICT
    alias: Optional[str] = None
    precision: Optional[float] = None
    case_insensitive: bool = False
    trim_whitespace: bool = False
    cast: Optional[CastType] = None
    pattern: Optional[str] = None
    datetime_format: Optional[str] = None
    datetime_tolerance: Optional[str] = None
    default: Any = None
    has_default: bool = False
    enum_map: Optional[dict] = None
    # Array rules
    array_mode: ArrayMode = ArrayMode.STRICT
    array_key: Optional[str | list[str]] = None
    order_by: Optional[list[str]] = None
    ignore_extra_items: bool = False
    ignore_missing_items: bool = False
    array_subset: bool = False
    duplicate_handling: DuplicateHandling = DuplicateHandling.ERROR
    # Global/inherited rules
    inherit_rules: bool = False
    when_condition: Optional[str] = None


@dataclass
class GlobalRules:
    """Global migration rules from schema root."""
    global_ignores: list[str] = field(default_factory=list)
    allow_null_as_missing: bool = False
    empty_string_as_null: bool = False


@dataclass
class ErrorResponse:
    """Error response structure."""
    success: bool = False
    error: Optional[dict] = None
    partial_result: Optional[DiffReport] = None

    def to_dict(self) -> dict:
        result = {"success": self.success}
        if self.error:
            result["error"] = self.error
        if self.partial_result:
            result["partial_result"] = self.partial_result.to_dict()
        return result

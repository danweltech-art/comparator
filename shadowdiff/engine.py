"""Main comparison engine for ShadowDiff."""

from __future__ import annotations

import time
import json
from datetime import datetime
from typing import Any, Optional

from .models import (
    EngineConfig,
    DiffReport,
    ExecutionInfo,
    Summary,
    Coverage,
    ErrorResponse,
)
from .schema import SchemaResolver, SchemaTraverser, RuleExtractor
from .normalizer import Normalizer
from .masker import Masker
from .differ import Differ
from .exceptions import (
    ValidationError,
    SchemaParseError,
    PayloadSizeError,
    MaxDepthExceededError,
    TimeoutError as EngineTimeoutError,
)
from .utils import get_json_size_mb


class ShadowDiffEngine:
    """
    Main comparison engine that orchestrates the 4-stage pipeline:

    1. Schema Resolution: Resolve $refs and build traversal map
    2. Normalization: Apply transformations (aliases, defaults, enum maps, sorting)
    3. Masking: Filter out ignored fields
    4. Typed Diffing: Deep comparison with type-aware rules
    """

    VERSION = "2.0.0"

    def __init__(self, config: Optional[EngineConfig] = None):
        """
        Initialize the engine.

        Args:
            config: Engine configuration (uses defaults if not provided)
        """
        self.config = config or EngineConfig()

    def compare(
        self,
        old_json: Any,
        new_json: Any,
        schema_fragment: dict
    ) -> DiffReport | ErrorResponse:
        """
        Compare two JSON payloads using the provided schema.

        Args:
            old_json: The baseline API response (typically from legacy system)
            new_json: The response to validate (typically from new system)
            schema_fragment: OpenAPI schema with x-migration-* extensions

        Returns:
            DiffReport on success, ErrorResponse on validation/processing errors
        """
        start_time = time.time()

        try:
            # Validate inputs
            self._validate_inputs(old_json, new_json, schema_fragment)

            # Stage 1: Schema Resolution
            resolver = SchemaResolver(schema_fragment, self.config.max_depth)
            resolved_schema = resolver.resolve()

            # Create traverser and extract global rules
            traverser = SchemaTraverser(resolved_schema)
            global_rules = RuleExtractor.extract_global_rules(resolved_schema)

            # Stage 2: Normalization
            normalizer = Normalizer(resolved_schema, global_rules, traverser)
            old_normalized, new_normalized = normalizer.normalize(old_json, new_json)

            # Stage 3: Masking
            masker = Masker(traverser)
            old_masked, new_masked, ignored_count = masker.mask(
                old_normalized, new_normalized
            )

            # Stage 4: Typed Diffing
            differ = Differ(
                traverser=traverser,
                root_data={'old': old_masked, 'new': new_masked},
                fail_fast=self.config.fail_fast,
                trace_rules=self.config.trace_rule_application
            )

            is_match = differ.diff(old_masked, new_masked)

            # Build report
            duration_ms = int((time.time() - start_time) * 1000)

            report = DiffReport(
                is_match=is_match and len(differ.diffs) == 0,
                execution=ExecutionInfo(
                    duration_ms=duration_ms,
                    timestamp=datetime.utcnow().isoformat() + "Z",
                    engine_version=self.VERSION
                ),
                summary=Summary(
                    total_fields_checked=differ.fields_checked,
                    mismatches_found=len(differ.diffs),
                    warnings_count=len(differ.warnings),
                    fields_ignored=ignored_count
                ),
                diffs=differ.diffs,
                warnings=differ.warnings,
                trace=differ.traces if self.config.trace_rule_application else []
            )

            # Add coverage if enabled
            if self.config.collect_statistics:
                report.coverage = self._calculate_coverage(
                    old_json, new_json, resolved_schema, differ
                )

            return report

        except ValidationError as e:
            return self._create_error_response(
                "VALIDATION_ERROR",
                e.message,
                e.details,
                start_time
            )
        except SchemaParseError as e:
            return self._create_error_response(
                "SCHEMA_PARSE_ERROR",
                e.message,
                {"line": e.line, "column": e.column, "reason": e.reason},
                start_time
            )
        except PayloadSizeError as e:
            return self._create_error_response(
                "PAYLOAD_SIZE_ERROR",
                str(e),
                {"size_mb": e.size_mb, "limit_mb": e.limit_mb},
                start_time
            )
        except MaxDepthExceededError as e:
            return self._create_error_response(
                "MAX_DEPTH_ERROR",
                str(e),
                {"depth": e.depth, "path": e.path},
                start_time
            )
        except Exception as e:
            return self._create_error_response(
                "PROCESSING_ERROR",
                str(e),
                {"type": type(e).__name__},
                start_time
            )

    def _validate_inputs(
        self,
        old_json: Any,
        new_json: Any,
        schema_fragment: dict
    ):
        """Validate input parameters."""
        # Check for None values
        if old_json is None:
            raise ValidationError("old_json is required")
        if new_json is None:
            raise ValidationError("new_json is required")
        if schema_fragment is None:
            raise ValidationError("schema_fragment is required")

        # Check schema is a dict
        if not isinstance(schema_fragment, dict):
            raise ValidationError(
                "schema_fragment must be an object",
                {"type": type(schema_fragment).__name__}
            )

        # Check payload sizes
        old_size = get_json_size_mb(old_json)
        new_size = get_json_size_mb(new_json)

        if old_size > self.config.max_payload_size_mb:
            raise PayloadSizeError(old_size, self.config.max_payload_size_mb)
        if new_size > self.config.max_payload_size_mb:
            raise PayloadSizeError(new_size, self.config.max_payload_size_mb)

    def _calculate_coverage(
        self,
        old_json: Any,
        new_json: Any,
        schema: dict,
        differ: Differ
    ) -> Coverage:
        """Calculate schema coverage statistics."""
        schema_fields = self._count_schema_fields(schema)
        payload_fields = max(
            self._count_payload_fields(old_json),
            self._count_payload_fields(new_json)
        )

        # Find unmatched fields
        old_paths = self._collect_paths(old_json, "$")
        new_paths = self._collect_paths(new_json, "$")

        unmatched_old = [p for p in old_paths if p not in new_paths]
        unmatched_new = [p for p in new_paths if p not in old_paths]

        return Coverage(
            fields_in_schema=schema_fields,
            fields_in_payload=payload_fields,
            unmatched_in_old=unmatched_old[:10],  # Limit for report size
            unmatched_in_new=unmatched_new[:10]
        )

    def _count_schema_fields(self, schema: dict, depth: int = 0) -> int:
        """Count fields defined in schema."""
        if depth > 50:
            return 0

        count = 0

        # Handle wrapped schema
        if 'components' in schema and 'schemas' in schema['components']:
            for s in schema['components']['schemas'].values():
                count += self._count_schema_fields(s, depth + 1)
            return count

        if 'properties' in schema:
            for prop_schema in schema['properties'].values():
                count += 1
                count += self._count_schema_fields(prop_schema, depth + 1)

        if 'items' in schema:
            count += self._count_schema_fields(schema['items'], depth + 1)

        return count

    def _count_payload_fields(self, data: Any, depth: int = 0) -> int:
        """Count fields in payload."""
        if depth > 50:
            return 0

        if isinstance(data, dict):
            count = len(data)
            for value in data.values():
                count += self._count_payload_fields(value, depth + 1)
            return count
        elif isinstance(data, list):
            count = 0
            for item in data:
                count += self._count_payload_fields(item, depth + 1)
            return count

        return 0

    def _collect_paths(self, data: Any, path: str, depth: int = 0) -> set:
        """Collect all field paths in data."""
        if depth > 50:
            return set()

        paths = set()

        if isinstance(data, dict):
            for key, value in data.items():
                child_path = f"{path}.{key}"
                paths.add(child_path)
                paths.update(self._collect_paths(value, child_path, depth + 1))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                child_path = f"{path}[{i}]"
                paths.update(self._collect_paths(item, child_path, depth + 1))

        return paths

    def _create_error_response(
        self,
        code: str,
        message: str,
        details: dict,
        start_time: float
    ) -> ErrorResponse:
        """Create an error response."""
        return ErrorResponse(
            success=False,
            error={
                "code": code,
                "message": message,
                "details": details
            },
            partial_result=None
        )


def compare(
    old_json: Any,
    new_json: Any,
    schema_fragment: dict,
    config: Optional[EngineConfig] = None
) -> DiffReport | ErrorResponse:
    """
    Convenience function to compare two JSON payloads.

    Args:
        old_json: The baseline API response
        new_json: The response to validate
        schema_fragment: OpenAPI schema with x-migration-* extensions
        config: Optional engine configuration

    Returns:
        DiffReport on success, ErrorResponse on errors
    """
    engine = ShadowDiffEngine(config)
    return engine.compare(old_json, new_json, schema_fragment)

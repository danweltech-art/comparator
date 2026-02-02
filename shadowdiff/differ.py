"""Stage 4: Typed Diffing (Deep Compare) for ShadowDiff engine."""

from __future__ import annotations

from typing import Any, Optional

from .models import (
    FieldRules,
    DiffEntry,
    DiffType,
    Severity,
    WarningEntry,
    TraceEntry,
    MigrationStrategy,
    ArrayMode,
)
from .schema import SchemaTraverser
from .normalizer import KeyedArrayTransformer
from .comparators import compare_with_rules
from .jsonpath_utils import evaluate_condition
from .utils import (
    get_type_name,
    values_equal,
    extract_key_value,
    format_key_value,
    build_path,
)


class Differ:
    """
    Performs typed deep comparison of normalized payloads.

    Handles:
    - Type-aware comparisons (numbers, strings, dates)
    - Array comparisons (strict, unordered, keyed)
    - Conditional rules (x-migration-when)
    - Existence checks (x-migration-strategy: exists)
    """

    def __init__(
        self,
        traverser: SchemaTraverser,
        root_data: Any,
        fail_fast: bool = False,
        trace_rules: bool = False
    ):
        self.traverser = traverser
        self.root_data = root_data  # For condition evaluation
        self.fail_fast = fail_fast
        self.trace_rules = trace_rules

        self.diffs: list[DiffEntry] = []
        self.warnings: list[WarningEntry] = []
        self.traces: list[TraceEntry] = []
        self.fields_checked = 0
        self._aborted = False

    def diff(
        self,
        old: Any,
        new: Any,
        path: str = "$",
        parent_rules: Optional[FieldRules] = None
    ) -> bool:
        """
        Perform deep diff comparison.

        Args:
            old: The old/baseline value
            new: The new value to validate
            path: Current JSONPath
            parent_rules: Parent field rules for inheritance

        Returns:
            True if values match, False otherwise
        """
        if self._aborted:
            return False

        rules = self.traverser.get_rules_for_path(path, parent_rules)

        # Check conditional rule
        if rules.when_condition:
            if not evaluate_condition(self.root_data, rules.when_condition):
                self._add_trace(path, 'x-migration-when', 'skipped',
                               {'condition': rules.when_condition})
                return True

        # Handle strategy: ignore
        if rules.strategy == MigrationStrategy.IGNORE:
            self._add_trace(path, 'x-migration-strategy', 'ignored')
            return True

        # Handle strategy: exists
        if rules.strategy == MigrationStrategy.EXISTS:
            self._add_trace(path, 'x-migration-strategy', 'exists-check')
            old_exists = old is not None
            new_exists = new is not None
            if old_exists != new_exists:
                self._add_diff(
                    path=path,
                    diff_type=DiffType.VALUE_MISMATCH,
                    old_value=old,
                    new_value=new,
                    message=f"Existence mismatch: old {'exists' if old_exists else 'missing'}, "
                           f"new {'exists' if new_exists else 'missing'}",
                    rule="x-migration-strategy: exists"
                )
                return False
            self.fields_checked += 1
            return True

        # Handle null values
        if old is None and new is None:
            self.fields_checked += 1
            return True

        if old is None:
            self._add_diff(
                path=path,
                diff_type=DiffType.MISSING_IN_NEW,
                old_value=old,
                new_value=new,
                message=f"Field missing in old, present in new: {new}"
            )
            return False

        if new is None:
            self._add_diff(
                path=path,
                diff_type=DiffType.MISSING_IN_NEW,
                old_value=old,
                new_value=new,
                message=f"Field present in old ({old}), missing in new"
            )
            return False

        # Apply type casting before type check
        if rules.cast:
            from .utils import safe_cast
            old = safe_cast(old, rules.cast.value)
            new = safe_cast(new, rules.cast.value)
            self._add_trace(path, 'x-migration-cast', 'applied',
                           {'cast_type': rules.cast.value})

        # Type check
        old_type = type(old)
        new_type = type(new)

        # Allow int/float interop
        if not (
            old_type == new_type or
            (isinstance(old, (int, float)) and isinstance(new, (int, float)) and
             not isinstance(old, bool) and not isinstance(new, bool))
        ):
            self._add_diff(
                path=path,
                diff_type=DiffType.TYPE_MISMATCH,
                old_value=old,
                new_value=new,
                message=f"Type mismatch: {get_type_name(old)} vs {get_type_name(new)}"
            )
            return False

        # Dispatch by type
        if isinstance(old, dict):
            return self._diff_objects(old, new, path, rules)
        elif isinstance(old, list):
            return self._diff_arrays(old, new, path, rules)
        else:
            return self._diff_scalars(old, new, path, rules)

    def _diff_objects(
        self,
        old: dict,
        new: dict,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Compare two objects."""
        all_match = True
        all_keys = set(old.keys()) | set(new.keys())

        for key in all_keys:
            if self._aborted:
                return False

            child_path = build_path(path, key)

            if key not in old:
                # Extra field in new
                child_rules = self.traverser.get_rules_for_path(child_path, rules)
                if child_rules.strategy != MigrationStrategy.IGNORE:
                    self._add_diff(
                        path=child_path,
                        diff_type=DiffType.EXTRA_IN_NEW,
                        old_value=None,
                        new_value=new[key],
                        message=f"Extra field in new: {key}"
                    )
                    all_match = False
                continue

            if key not in new:
                # Missing field in new
                child_rules = self.traverser.get_rules_for_path(child_path, rules)
                if child_rules.strategy != MigrationStrategy.IGNORE:
                    # Check for default
                    if child_rules.has_default:
                        # Compare old value with default
                        if not self.diff(old[key], child_rules.default, child_path, rules):
                            all_match = False
                    else:
                        self._add_diff(
                            path=child_path,
                            diff_type=DiffType.MISSING_IN_NEW,
                            old_value=old[key],
                            new_value=None,
                            message=f"Field missing in new: {key}"
                        )
                        all_match = False
                continue

            # Both have the key - recurse
            if not self.diff(old[key], new[key], child_path, rules):
                all_match = False

        return all_match

    def _diff_arrays(
        self,
        old: list,
        new: list,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Compare two arrays based on array mode."""
        if rules.array_mode == ArrayMode.KEYED:
            return self._diff_keyed_arrays(old, new, path, rules)
        elif rules.array_mode == ArrayMode.UNORDERED:
            return self._diff_unordered_arrays(old, new, path, rules)
        else:
            return self._diff_strict_arrays(old, new, path, rules)

    def _diff_strict_arrays(
        self,
        old: list,
        new: list,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Compare arrays index-by-index (order matters)."""
        all_match = True

        # Check length
        if len(old) != len(new):
            if not rules.ignore_extra_items and len(new) > len(old):
                self._add_diff(
                    path=path,
                    diff_type=DiffType.ARRAY_LENGTH_MISMATCH,
                    old_value=len(old),
                    new_value=len(new),
                    message=f"Array length mismatch: {len(old)} vs {len(new)}"
                )
                if not rules.ignore_extra_items:
                    all_match = False
            elif not rules.ignore_missing_items and len(old) > len(new):
                self._add_diff(
                    path=path,
                    diff_type=DiffType.ARRAY_LENGTH_MISMATCH,
                    old_value=len(old),
                    new_value=len(new),
                    message=f"Array length mismatch: {len(old)} vs {len(new)}"
                )
                all_match = False

        # Compare common elements
        min_len = min(len(old), len(new))
        for i in range(min_len):
            if self._aborted:
                return False

            child_path = f"{path}[{i}]"
            if not self.diff(old[i], new[i], child_path, rules):
                all_match = False

        # Handle extra items in new
        if len(new) > len(old):
            if rules.ignore_extra_items:
                self._add_warning(
                    path=path,
                    diff_type=DiffType.ARRAY_ITEM_EXTRA,
                    message=f"New array contains {len(new) - len(old)} extra items "
                           f"(allowed by x-migration-ignore-extra-items)"
                )
            else:
                for i in range(len(old), len(new)):
                    self._add_diff(
                        path=f"{path}[{i}]",
                        diff_type=DiffType.ARRAY_ITEM_EXTRA,
                        old_value=None,
                        new_value=new[i],
                        message=f"Extra item in new array at index {i}"
                    )
                all_match = False

        # Handle missing items in new
        if len(old) > len(new) and not rules.ignore_missing_items:
            for i in range(len(new), len(old)):
                self._add_diff(
                    path=f"{path}[{i}]",
                    diff_type=DiffType.ARRAY_ITEM_MISSING,
                    old_value=old[i],
                    new_value=None,
                    message=f"Missing item in new array at index {i}"
                )
            all_match = False

        return all_match

    def _diff_unordered_arrays(
        self,
        old: list,
        new: list,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Compare arrays as sets (order ignored, duplicates matter)."""
        all_match = True

        # For objects, we need to do pairwise matching
        # For primitives, we can use direct comparison
        old_matched = [False] * len(old)
        new_matched = [False] * len(new)

        # Try to match each old item with a new item
        for i, old_item in enumerate(old):
            for j, new_item in enumerate(new):
                if new_matched[j]:
                    continue

                if self._items_equal(old_item, new_item, f"{path}[{i}]", rules):
                    old_matched[i] = True
                    new_matched[j] = True
                    self.fields_checked += 1
                    break

        # Report unmatched old items
        for i, matched in enumerate(old_matched):
            if not matched:
                if not rules.ignore_missing_items and not rules.array_subset:
                    self._add_diff(
                        path=f"{path}[{i}]",
                        diff_type=DiffType.ARRAY_ITEM_MISSING,
                        old_value=old[i],
                        new_value=None,
                        message=f"Item from old array not found in new"
                    )
                    all_match = False

        # Report unmatched new items
        for j, matched in enumerate(new_matched):
            if not matched:
                if rules.ignore_extra_items:
                    self._add_warning(
                        path=f"{path}[{j}]",
                        diff_type=DiffType.ARRAY_ITEM_EXTRA,
                        message=f"Extra item in new array (allowed by x-migration-ignore-extra-items)"
                    )
                else:
                    self._add_diff(
                        path=f"{path}[{j}]",
                        diff_type=DiffType.ARRAY_ITEM_EXTRA,
                        old_value=None,
                        new_value=new[j],
                        message=f"Extra item in new array"
                    )
                    all_match = False

        return all_match

    def _diff_keyed_arrays(
        self,
        old: list,
        new: list,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Compare arrays by matching objects using a key field."""
        all_match = True

        transformer = KeyedArrayTransformer(self.traverser)
        old_map, new_map, duplicates = transformer.transform(old, new, rules)

        # Report duplicate key errors
        for dup in duplicates:
            self._add_diff(
                path=path,
                diff_type=DiffType.DUPLICATE_KEY,
                old_value=None,
                new_value=None,
                message=f"Duplicate key {format_key_value(dup['key'])} at indices {dup['indices']}",
                rule=f"x-migration-array-key: {rules.array_key}"
            )
            all_match = False

        # Compare matched items
        all_keys = set(old_map.keys()) | set(new_map.keys())
        key_spec = rules.array_key or "id"

        for key in all_keys:
            if self._aborted:
                return False

            key_display = format_key_value(key)
            # Build a JSONPath-like path for keyed items
            if isinstance(key_spec, str):
                item_path = f"{path}[?(@.{key_spec}=={key_display})]"
            else:
                item_path = f"{path}[key={key_display}]"

            if key not in old_map:
                if rules.ignore_extra_items:
                    self._add_warning(
                        path=item_path,
                        diff_type=DiffType.ARRAY_ITEM_EXTRA,
                        message=f"Extra item with key {key_display} in new (allowed)"
                    )
                else:
                    self._add_diff(
                        path=item_path,
                        diff_type=DiffType.ARRAY_ITEM_EXTRA,
                        old_value=None,
                        new_value=new_map[key],
                        message=f"Extra item with key {key_display} in new array"
                    )
                    all_match = False
                continue

            if key not in new_map:
                if rules.ignore_missing_items:
                    self._add_warning(
                        path=item_path,
                        diff_type=DiffType.ARRAY_ITEM_MISSING,
                        message=f"Missing item with key {key_display} in new (allowed)"
                    )
                else:
                    self._add_diff(
                        path=item_path,
                        diff_type=DiffType.ARRAY_ITEM_MISSING,
                        old_value=old_map[key],
                        new_value=None,
                        message=f"Missing item with key {key_display} in new array"
                    )
                    all_match = False
                continue

            # Both have the key - compare the objects
            if not self.diff(old_map[key], new_map[key], item_path, rules):
                all_match = False

        return all_match

    def _diff_scalars(
        self,
        old: Any,
        new: Any,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Compare scalar values."""
        self.fields_checked += 1

        # Apply lenient comparison if specified
        if rules.strategy == MigrationStrategy.LENIENT:
            # For lenient mode, use trim_whitespace and case_insensitive
            rules.trim_whitespace = True
            rules.case_insensitive = True

        is_match, message = compare_with_rules(old, new, rules)

        if is_match:
            if rules.precision is not None:
                self._add_trace(path, 'x-migration-precision', 'matched',
                               {'precision': rules.precision})
            return True

        # Determine diff type based on rules
        diff_type = DiffType.VALUE_MISMATCH

        if rules.precision is not None:
            diff_type = DiffType.PRECISION_EXCEEDED
            rule_applied = f"x-migration-precision: {rules.precision}"
        elif rules.pattern is not None:
            diff_type = DiffType.PATTERN_MISMATCH
            rule_applied = f"x-migration-pattern: {rules.pattern}"
        elif rules.datetime_tolerance is not None:
            diff_type = DiffType.DATETIME_EXCEEDED
            rule_applied = f"x-migration-datetime-tolerance: {rules.datetime_tolerance}"
        else:
            rule_applied = None

        self._add_diff(
            path=path,
            diff_type=diff_type,
            old_value=old,
            new_value=new,
            message=message,
            rule=rule_applied
        )

        return False

    def _items_equal(
        self,
        old: Any,
        new: Any,
        path: str,
        rules: FieldRules
    ) -> bool:
        """Check if two items are equal (for unordered array comparison)."""
        # Create a temporary differ to avoid polluting our diffs
        temp_differ = Differ(
            self.traverser,
            self.root_data,
            fail_fast=True,
            trace_rules=False
        )
        return temp_differ.diff(old, new, path, rules)

    def _add_diff(
        self,
        path: str,
        diff_type: DiffType,
        old_value: Any,
        new_value: Any,
        message: str,
        rule: str = None
    ):
        """Add a diff entry."""
        self.diffs.append(DiffEntry(
            path=path,
            type=diff_type,
            severity=Severity.ERROR,
            old_value=old_value,
            new_value=new_value,
            message=message,
            rule_applied=rule
        ))

        if self.fail_fast:
            self._aborted = True

    def _add_warning(
        self,
        path: str,
        diff_type: DiffType,
        message: str
    ):
        """Add a warning entry."""
        self.warnings.append(WarningEntry(
            path=path,
            type=diff_type,
            severity=Severity.WARNING,
            message=message
        ))

    def _add_trace(
        self,
        path: str,
        rule: str,
        action: str,
        details: dict = None
    ):
        """Add a trace entry if tracing is enabled."""
        if self.trace_rules:
            self.traces.append(TraceEntry(
                path=path,
                rule=rule,
                action=action,
                details=details
            ))

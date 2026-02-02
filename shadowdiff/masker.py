"""Stage 3: Masking ("The Filter") for ShadowDiff engine."""

from __future__ import annotations

from typing import Any
from copy import deepcopy

from .models import FieldRules, MigrationStrategy
from .schema import SchemaTraverser


class Masker:
    """
    Applies masking rules to filter out ignored fields.

    Operations:
    - Remove fields with x-migration-strategy: ignore
    - Propagate inherited rules to children
    """

    def __init__(self, traverser: SchemaTraverser):
        self.traverser = traverser
        self.ignored_count = 0

    def mask(
        self,
        old_json: Any,
        new_json: Any
    ) -> tuple[Any, Any, int]:
        """
        Apply masking to both payloads.

        Args:
            old_json: The old/baseline payload
            new_json: The new payload

        Returns:
            Tuple of (masked_old, masked_new, ignored_count)
        """
        self.ignored_count = 0

        old = self._mask_recursive(deepcopy(old_json), "$", None)
        new = self._mask_recursive(deepcopy(new_json), "$", None)

        return old, new, self.ignored_count

    def _mask_recursive(
        self,
        data: Any,
        path: str,
        parent_rules: FieldRules | None
    ) -> Any:
        """Recursively apply masking rules."""
        rules = self.traverser.get_rules_for_path(path, parent_rules)

        # Check if this field should be ignored
        if rules.strategy == MigrationStrategy.IGNORE:
            self.ignored_count += 1
            return None  # Signal to remove this field

        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                child_path = f"{path}.{key}"
                masked_value = self._mask_recursive(value, child_path, rules)
                if masked_value is not None or not self._was_ignored(child_path):
                    result[key] = masked_value
            return result

        elif isinstance(data, list):
            result = []
            for i, item in enumerate(data):
                child_path = f"{path}[{i}]"
                masked_value = self._mask_recursive(item, child_path, rules)
                # For arrays, we keep the structure but masked content
                result.append(masked_value)
            return result

        return data

    def _was_ignored(self, path: str) -> bool:
        """Check if a path was ignored due to strategy."""
        rules = self.traverser.get_rules_for_path(path, None)
        return rules.strategy == MigrationStrategy.IGNORE

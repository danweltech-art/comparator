"""Stage 2: Normalization ("The Scrub") for ShadowDiff engine."""

from __future__ import annotations

from typing import Any, Optional
from copy import deepcopy

from .models import GlobalRules, FieldRules, ArrayMode, DuplicateHandling
from .schema import SchemaTraverser
from .jsonpath_utils import JSONPathMatcher
from .utils import extract_key_value, merge_dicts


class Normalizer:
    """
    Normalizes payloads according to schema rules.

    Stages:
    1. Global Ignores: Delete all paths matching x-migration-global-ignores
    2. Alias Resolution: Rename Old payload keys per x-migration-alias
    3. Default Injection: Insert x-migration-default values for missing fields
    4. Enum Mapping: Translate Old values per x-migration-enum-map
    5. Array Sorting: Sort arrays by x-migration-order-by
    6. Keyed Array Transform: Convert to keyed structure if array-mode: keyed
    """

    def __init__(
        self,
        schema: dict,
        global_rules: GlobalRules,
        traverser: SchemaTraverser
    ):
        self.schema = schema
        self.global_rules = global_rules
        self.traverser = traverser

    def normalize(
        self,
        old_json: Any,
        new_json: Any
    ) -> tuple[Any, Any]:
        """
        Normalize both payloads.

        Args:
            old_json: The old/baseline payload
            new_json: The new payload to validate

        Returns:
            Tuple of (normalized_old, normalized_new)
        """
        old = deepcopy(old_json)
        new = deepcopy(new_json)

        # Stage 2.1: Apply global ignores
        old = self._apply_global_ignores(old)
        new = self._apply_global_ignores(new)

        # Stage 2.2: Apply alias resolution (old payload only)
        old = self._apply_aliases(old, "$")

        # Stage 2.3: Apply null-as-missing normalization
        if self.global_rules.allow_null_as_missing:
            old = self._normalize_nulls(old)
            new = self._normalize_nulls(new)

        # Stage 2.4: Apply empty-string-as-null normalization
        if self.global_rules.empty_string_as_null:
            old = self._normalize_empty_strings(old)
            new = self._normalize_empty_strings(new)

        # Stage 2.5: Apply default injection
        old = self._apply_defaults(old, "$")
        new = self._apply_defaults(new, "$")

        # Stage 2.6: Apply enum mapping (old payload only)
        old = self._apply_enum_mapping(old, "$")

        # Stage 2.7: Apply array sorting
        old = self._apply_array_sorting(old, "$")
        new = self._apply_array_sorting(new, "$")

        return old, new

    def _apply_global_ignores(self, data: Any) -> Any:
        """Delete all paths matching global ignores."""
        if not self.global_rules.global_ignores:
            return data

        for ignore_path in self.global_rules.global_ignores:
            data = self._delete_jsonpath(data, ignore_path)

        return data

    def _delete_jsonpath(self, data: Any, path: str) -> Any:
        """Delete nodes matching a JSONPath pattern."""
        # Handle recursive descent pattern $..field
        if '..' in path:
            return self._delete_recursive(data, path)

        # Use JSONPathMatcher for standard patterns
        return JSONPathMatcher.delete_paths(data, [path])

    def _delete_recursive(self, data: Any, pattern: str) -> Any:
        """Delete fields matching recursive descent pattern."""
        # Extract the field name from $..field pattern
        if '..' not in pattern:
            return data

        parts = pattern.split('..')
        if len(parts) != 2:
            return data

        field_name = parts[1].lstrip('.')

        # Handle array notation like ['@odata.context']
        if field_name.startswith('[') and field_name.endswith(']'):
            field_name = field_name[2:-2]  # Remove [' and ']

        return self._delete_field_recursive(data, field_name)

    def _delete_field_recursive(self, data: Any, field: str) -> Any:
        """Recursively delete a field from nested structures."""
        if isinstance(data, dict):
            # Delete the field if present
            if field in data:
                del data[field]

            # Recurse into remaining values
            for key in list(data.keys()):
                data[key] = self._delete_field_recursive(data[key], field)

        elif isinstance(data, list):
            for i, item in enumerate(data):
                data[i] = self._delete_field_recursive(item, field)

        return data

    def _apply_aliases(self, data: Any, path: str) -> Any:
        """Apply alias mappings to rename fields in old payload."""
        if not isinstance(data, dict):
            if isinstance(data, list):
                return [
                    self._apply_aliases(item, f"{path}[{i}]")
                    for i, item in enumerate(data)
                ]
            return data

        result = {}
        for key, value in data.items():
            child_path = f"{path}.{key}"
            schema_node = self.traverser.get_schema_for_path(child_path)

            # Check all schema properties for aliases pointing to this key
            new_key = key
            if schema_node is None:
                # Look for alias in parent schema
                parent_schema = self.traverser.get_schema_for_path(path)
                if parent_schema and 'properties' in parent_schema:
                    for prop_name, prop_schema in parent_schema['properties'].items():
                        alias = prop_schema.get('x-migration-alias')
                        if alias == key:
                            new_key = prop_name
                            break

            # Recursively process the value
            result[new_key] = self._apply_aliases(value, child_path)

        return result

    def _normalize_nulls(self, data: Any) -> Any:
        """Remove null values (treat null as missing)."""
        if isinstance(data, dict):
            return {
                k: self._normalize_nulls(v)
                for k, v in data.items()
                if v is not None
            }
        elif isinstance(data, list):
            return [self._normalize_nulls(item) for item in data]
        return data

    def _normalize_empty_strings(self, data: Any) -> Any:
        """Convert empty strings to null."""
        if isinstance(data, dict):
            return {
                k: self._normalize_empty_strings(v)
                for k, v in data.items()
            }
        elif isinstance(data, list):
            return [self._normalize_empty_strings(item) for item in data]
        elif isinstance(data, str) and data == "":
            return None
        return data

    def _apply_defaults(self, data: Any, path: str) -> Any:
        """Inject default values for missing fields."""
        schema_node = self.traverser.get_schema_for_path(path)

        if schema_node is None:
            return data

        if isinstance(data, dict):
            # Check for missing properties with defaults
            if 'properties' in schema_node:
                for prop_name, prop_schema in schema_node['properties'].items():
                    if prop_name not in data:
                        if 'x-migration-default' in prop_schema:
                            data[prop_name] = prop_schema['x-migration-default']

            # Recurse into existing properties
            result = {}
            for key, value in data.items():
                child_path = f"{path}.{key}"
                result[key] = self._apply_defaults(value, child_path)
            return result

        elif isinstance(data, list):
            return [
                self._apply_defaults(item, f"{path}[{i}]")
                for i, item in enumerate(data)
            ]

        return data

    def _apply_enum_mapping(self, data: Any, path: str) -> Any:
        """Apply enum value mappings to old payload."""
        schema_node = self.traverser.get_schema_for_path(path)

        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                child_path = f"{path}.{key}"
                result[key] = self._apply_enum_mapping(value, child_path)
            return result

        elif isinstance(data, list):
            return [
                self._apply_enum_mapping(item, f"{path}[{i}]")
                for i, item in enumerate(data)
            ]

        elif schema_node:
            enum_map = schema_node.get('x-migration-enum-map')
            if enum_map and data in enum_map:
                return enum_map[data]

        return data

    def _apply_array_sorting(self, data: Any, path: str) -> Any:
        """Sort arrays by order-by specification."""
        schema_node = self.traverser.get_schema_for_path(path)

        if isinstance(data, list):
            order_by = None
            if schema_node:
                order_by = schema_node.get('x-migration-order-by')

            if order_by:
                data = self._sort_array(data, order_by)

            # Recurse into array items
            return [
                self._apply_array_sorting(item, f"{path}[{i}]")
                for i, item in enumerate(data)
            ]

        elif isinstance(data, dict):
            return {
                k: self._apply_array_sorting(v, f"{path}.{k}")
                for k, v in data.items()
            }

        return data

    def _sort_array(self, array: list, order_by: list[str]) -> list:
        """Sort an array by the specified fields."""
        if not array or not all(isinstance(item, dict) for item in array):
            return array

        def sort_key(item):
            keys = []
            for field in order_by:
                descending = field.startswith('-')
                field_name = field[1:] if descending else field
                value = item.get(field_name)

                # Handle None values
                if value is None:
                    value = "" if isinstance(item.get(field_name), str) else 0

                # Negate for descending order (works for numbers)
                if descending and isinstance(value, (int, float)):
                    value = -value
                elif descending and isinstance(value, str):
                    # For strings, we'll need to reverse sort separately
                    pass

                keys.append((0 if not descending else 1, value))

            return keys

        try:
            return sorted(array, key=sort_key)
        except TypeError:
            # If comparison fails, return unsorted
            return array


class KeyedArrayTransformer:
    """Transforms arrays with keyed mode for comparison."""

    def __init__(self, traverser: SchemaTraverser):
        self.traverser = traverser

    def transform(
        self,
        old_array: list,
        new_array: list,
        rules: FieldRules
    ) -> tuple[dict, dict, list]:
        """
        Transform arrays to keyed maps for comparison.

        Args:
            old_array: The old array
            new_array: The new array
            rules: Field rules with array configuration

        Returns:
            Tuple of (old_map, new_map, duplicate_errors)
        """
        key_spec = rules.array_key
        if not key_spec:
            # No key specified, use index as key
            old_map = {i: item for i, item in enumerate(old_array)}
            new_map = {i: item for i, item in enumerate(new_array)}
            return old_map, new_map, []

        old_map, old_dups = self._build_key_map(
            old_array, key_spec, rules.duplicate_handling
        )
        new_map, new_dups = self._build_key_map(
            new_array, key_spec, rules.duplicate_handling
        )

        return old_map, new_map, old_dups + new_dups

    def _build_key_map(
        self,
        array: list,
        key_spec: str | list[str],
        dup_handling: DuplicateHandling
    ) -> tuple[dict, list]:
        """Build a map from key values to array items."""
        result = {}
        duplicates = []

        for i, item in enumerate(array):
            key_value = extract_key_value(item, key_spec)

            if key_value is None:
                continue

            if key_value in result:
                if dup_handling == DuplicateHandling.ERROR:
                    duplicates.append({
                        'key': key_value,
                        'indices': [result[key_value]['_index'], i]
                    })
                elif dup_handling == DuplicateHandling.FIRST:
                    continue  # Keep first occurrence
                elif dup_handling == DuplicateHandling.LAST:
                    result[key_value] = {**item, '_index': i}
                elif dup_handling == DuplicateHandling.MERGE:
                    result[key_value] = merge_dicts(result[key_value], item)
                    result[key_value]['_index'] = i
            else:
                result[key_value] = {**item, '_index': i}

        # Remove _index markers
        for key in result:
            if '_index' in result[key]:
                del result[key]['_index']

        return result, duplicates

"""Schema resolution and rule extraction for ShadowDiff engine."""

from __future__ import annotations

from typing import Any, Optional
from copy import deepcopy

from .models import (
    FieldRules,
    GlobalRules,
    MigrationStrategy,
    ArrayMode,
    DuplicateHandling,
    CastType,
)
from .exceptions import ExternalRefError, CircularRefError, SchemaParseError


class SchemaResolver:
    """Resolves $ref references within an OpenAPI schema fragment."""

    def __init__(self, schema: dict, max_depth: int = 100):
        self.schema = schema
        self.max_depth = max_depth
        self._resolution_stack: list[str] = []

    def resolve(self) -> dict:
        """
        Resolve all $ref references in the schema.

        Returns:
            Fully resolved schema with all $refs inlined
        """
        return self._resolve_node(self.schema, depth=0)

    def _resolve_node(self, node: Any, depth: int) -> Any:
        """Recursively resolve a schema node."""
        if depth > self.max_depth:
            return node

        if isinstance(node, dict):
            if '$ref' in node:
                return self._resolve_ref(node['$ref'], depth)

            resolved = {}
            for key, value in node.items():
                resolved[key] = self._resolve_node(value, depth + 1)
            return resolved

        elif isinstance(node, list):
            return [self._resolve_node(item, depth + 1) for item in node]

        return node

    def _resolve_ref(self, ref: str, depth: int) -> dict:
        """Resolve a $ref reference."""
        # Check for external references
        if ref.startswith('http://') or ref.startswith('https://'):
            raise ExternalRefError(ref)

        if not ref.startswith('#/'):
            raise ExternalRefError(ref)

        # Check for circular references
        if ref in self._resolution_stack:
            raise CircularRefError(ref)

        self._resolution_stack.append(ref)

        try:
            # Parse the reference path
            path_parts = ref[2:].split('/')  # Remove '#/' prefix
            resolved = self.schema

            for part in path_parts:
                # Handle JSON pointer escaping
                part = part.replace('~1', '/').replace('~0', '~')
                if isinstance(resolved, dict) and part in resolved:
                    resolved = resolved[part]
                else:
                    raise SchemaParseError(
                        f"Cannot resolve $ref: {ref}",
                        reason=f"Path component '{part}' not found"
                    )

            # Recursively resolve the referenced schema
            return self._resolve_node(deepcopy(resolved), depth + 1)

        finally:
            self._resolution_stack.pop()


class RuleExtractor:
    """Extracts migration rules from schema properties."""

    @staticmethod
    def extract_field_rules(
        schema_node: dict,
        parent_rules: Optional[FieldRules] = None
    ) -> FieldRules:
        """
        Extract field-level migration rules from a schema node.

        Args:
            schema_node: The schema property node
            parent_rules: Parent rules to inherit from (if inherit_rules is true)

        Returns:
            FieldRules object with all extracted rules
        """
        rules = FieldRules()

        # Check for rule inheritance
        if parent_rules and parent_rules.inherit_rules:
            rules = FieldRules(
                strategy=parent_rules.strategy,
                case_insensitive=parent_rules.case_insensitive,
                trim_whitespace=parent_rules.trim_whitespace,
                inherit_rules=True,
            )

        # x-migration-strategy
        strategy = schema_node.get('x-migration-strategy', 'strict')
        if strategy in [s.value for s in MigrationStrategy]:
            rules.strategy = MigrationStrategy(strategy)

        # x-migration-alias
        rules.alias = schema_node.get('x-migration-alias')

        # x-migration-precision
        precision = schema_node.get('x-migration-precision')
        if precision is not None:
            rules.precision = float(precision)

        # x-migration-case-insensitive
        rules.case_insensitive = schema_node.get(
            'x-migration-case-insensitive',
            rules.case_insensitive
        )

        # x-migration-trim-whitespace
        rules.trim_whitespace = schema_node.get(
            'x-migration-trim-whitespace',
            rules.trim_whitespace
        )

        # x-migration-cast
        cast = schema_node.get('x-migration-cast')
        if cast and cast in [c.value for c in CastType]:
            rules.cast = CastType(cast)

        # x-migration-pattern
        rules.pattern = schema_node.get('x-migration-pattern')

        # x-migration-datetime-format
        rules.datetime_format = schema_node.get('x-migration-datetime-format')

        # x-migration-datetime-tolerance
        rules.datetime_tolerance = schema_node.get('x-migration-datetime-tolerance')

        # x-migration-default
        if 'x-migration-default' in schema_node:
            rules.default = schema_node['x-migration-default']
            rules.has_default = True

        # x-migration-enum-map
        rules.enum_map = schema_node.get('x-migration-enum-map')

        # Array rules
        array_mode = schema_node.get('x-migration-array-mode', 'strict')
        if array_mode in [m.value for m in ArrayMode]:
            rules.array_mode = ArrayMode(array_mode)

        rules.array_key = schema_node.get('x-migration-array-key')
        rules.order_by = schema_node.get('x-migration-order-by')
        rules.ignore_extra_items = schema_node.get('x-migration-ignore-extra-items', False)
        rules.ignore_missing_items = schema_node.get('x-migration-ignore-missing-items', False)
        rules.array_subset = schema_node.get('x-migration-array-subset', False)

        dup_handling = schema_node.get('x-migration-duplicate-handling', 'error')
        if dup_handling in [d.value for d in DuplicateHandling]:
            rules.duplicate_handling = DuplicateHandling(dup_handling)

        # Global/inherited rules
        rules.inherit_rules = schema_node.get('x-migration-inherit-rules', False)
        rules.when_condition = schema_node.get('x-migration-when')

        return rules

    @staticmethod
    def extract_global_rules(schema: dict) -> GlobalRules:
        """
        Extract global migration rules from the schema root.

        Args:
            schema: The root schema object

        Returns:
            GlobalRules object
        """
        rules = GlobalRules()

        # Look for global rules in various places
        root = schema

        # Check if this is wrapped in components/schemas
        if 'components' in schema and 'schemas' in schema['components']:
            # Find the first schema
            schemas = schema['components']['schemas']
            if schemas:
                first_schema = next(iter(schemas.values()))
                root = first_schema

        # x-migration-global-ignores
        rules.global_ignores = root.get('x-migration-global-ignores', [])

        # x-migration-allow-null-as-missing
        rules.allow_null_as_missing = root.get('x-migration-allow-null-as-missing', False)

        # x-migration-empty-string-as-null
        rules.empty_string_as_null = root.get('x-migration-empty-string-as-null', False)

        return rules


class SchemaTraverser:
    """Traverses schema and payload in parallel to find matching rules."""

    def __init__(self, schema: dict):
        self.schema = schema
        self.rule_extractor = RuleExtractor()
        self._schema_cache: dict[str, Optional[dict]] = {}

    def get_schema_for_path(self, path: str) -> Optional[dict]:
        """
        Get the schema node for a given JSONPath.

        Args:
            path: The JSONPath (e.g., '$.user.name')

        Returns:
            Schema node or None if not found
        """
        if path in self._schema_cache:
            return self._schema_cache[path]

        result = self._traverse_to_path(path)
        self._schema_cache[path] = result
        return result

    def _traverse_to_path(self, path: str) -> Optional[dict]:
        """Traverse schema to find node for path."""
        if path == '$':
            return self._get_root_schema()

        # Parse path segments
        segments = self._parse_path_segments(path)
        if not segments:
            return self._get_root_schema()

        current = self._get_root_schema()
        if not current:
            return None

        for segment in segments:
            if current is None:
                return None

            if isinstance(segment, int):
                # Array index - get items schema
                if current.get('type') == 'array' and 'items' in current:
                    current = current['items']
                else:
                    return None
            else:
                # Object property
                if current.get('type') == 'object' or 'properties' in current:
                    props = current.get('properties', {})
                    if segment in props:
                        current = props[segment]
                    elif 'additionalProperties' in current:
                        current = current['additionalProperties']
                    else:
                        return None
                else:
                    return None

        return current

    def _get_root_schema(self) -> Optional[dict]:
        """Get the root schema object."""
        schema = self.schema

        # Handle wrapped schemas
        if 'components' in schema and 'schemas' in schema['components']:
            schemas = schema['components']['schemas']
            if schemas:
                return next(iter(schemas.values()))

        # Check for type/properties at root
        if 'type' in schema or 'properties' in schema:
            return schema

        return schema

    def _parse_path_segments(self, path: str) -> list:
        """Parse JSONPath into segments."""
        import re

        if path == '$':
            return []

        # Remove leading $.
        if path.startswith('$.'):
            path = path[2:]
        elif path.startswith('$'):
            path = path[1:]

        segments = []
        # Match either .name or [index] or ['name']
        pattern = r"\.([^.\[\]]+)|\[(\d+)\]|\['([^']+)'\]|\[\"([^\"]+)\"\]"

        pos = 0
        # Handle initial segment without dot
        if path and not path.startswith('[') and not path.startswith('.'):
            match = re.match(r'^([^.\[\]]+)', path)
            if match:
                segments.append(match.group(1))
                pos = match.end()

        for match in re.finditer(pattern, path[pos:]):
            if match.group(1) is not None:
                segments.append(match.group(1))
            elif match.group(2) is not None:
                segments.append(int(match.group(2)))
            elif match.group(3) is not None:
                segments.append(match.group(3))
            elif match.group(4) is not None:
                segments.append(match.group(4))

        return segments

    def get_rules_for_path(
        self,
        path: str,
        parent_rules: Optional[FieldRules] = None
    ) -> FieldRules:
        """
        Get the migration rules for a given path.

        Args:
            path: The JSONPath
            parent_rules: Parent rules for inheritance

        Returns:
            FieldRules for the path
        """
        schema_node = self.get_schema_for_path(path)
        if schema_node:
            return self.rule_extractor.extract_field_rules(schema_node, parent_rules)
        return FieldRules()

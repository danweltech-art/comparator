"""JSONPath utilities for ShadowDiff engine."""

from __future__ import annotations

import re
from typing import Any, Generator, Optional
from jsonpath_ng import parse as jsonpath_parse
from jsonpath_ng.exceptions import JsonPathParserError


class JSONPathMatcher:
    """Utility class for JSONPath matching and manipulation."""

    # Cache for compiled JSONPath expressions
    _cache: dict = {}

    @classmethod
    def compile(cls, path: str):
        """Compile and cache a JSONPath expression."""
        if path not in cls._cache:
            try:
                cls._cache[path] = jsonpath_parse(path)
            except JsonPathParserError as e:
                raise ValueError(f"Invalid JSONPath expression '{path}': {e}")
        return cls._cache[path]

    @classmethod
    def find_all(cls, data: Any, path: str) -> list[tuple[str, Any]]:
        """
        Find all matches for a JSONPath expression.

        Returns:
            List of (full_path, value) tuples
        """
        try:
            expr = cls.compile(path)
            matches = expr.find(data)
            return [(str(m.full_path), m.value) for m in matches]
        except Exception:
            return []

    @classmethod
    def find_values(cls, data: Any, path: str) -> list[Any]:
        """Find all values matching a JSONPath expression."""
        try:
            expr = cls.compile(path)
            return [m.value for m in expr.find(data)]
        except Exception:
            return []

    @classmethod
    def delete_paths(cls, data: Any, paths: list[str]) -> Any:
        """
        Delete all paths matching the given JSONPath expressions.

        Args:
            data: The data to modify (will be modified in place)
            paths: List of JSONPath expressions

        Returns:
            Modified data
        """
        for path in paths:
            data = cls._delete_path(data, path)
        return data

    @classmethod
    def _delete_path(cls, data: Any, path: str) -> Any:
        """Delete a single JSONPath from data."""
        try:
            expr = cls.compile(path)
            matches = expr.find(data)

            # Process matches in reverse order to avoid index issues
            for match in reversed(matches):
                parent_path = match.full_path
                if hasattr(parent_path, 'left') and hasattr(parent_path, 'right'):
                    parent = parent_path.left.find(data)
                    if parent:
                        parent_obj = parent[0].value
                        key = parent_path.right
                        if hasattr(key, 'index'):
                            # Array index
                            if isinstance(parent_obj, list) and 0 <= key.index < len(parent_obj):
                                del parent_obj[key.index]
                        elif hasattr(key, 'fields'):
                            # Object field
                            for field in key.fields:
                                if isinstance(parent_obj, dict) and field in parent_obj:
                                    del parent_obj[field]
        except Exception:
            pass

        return data

    @classmethod
    def set_value(cls, data: Any, path: str, value: Any) -> Any:
        """
        Set a value at the given JSONPath.

        Note: This is a simplified implementation that works for basic paths.
        """
        # Parse path segments
        segments = cls._parse_path_segments(path)
        if not segments:
            return value

        current = data
        for i, segment in enumerate(segments[:-1]):
            if isinstance(segment, int):
                if isinstance(current, list) and 0 <= segment < len(current):
                    current = current[segment]
                else:
                    return data
            else:
                if isinstance(current, dict):
                    if segment not in current:
                        # Create intermediate objects/arrays
                        next_seg = segments[i + 1]
                        current[segment] = [] if isinstance(next_seg, int) else {}
                    current = current[segment]
                else:
                    return data

        # Set the final value
        final_segment = segments[-1]
        if isinstance(final_segment, int):
            if isinstance(current, list):
                while len(current) <= final_segment:
                    current.append(None)
                current[final_segment] = value
        else:
            if isinstance(current, dict):
                current[final_segment] = value

        return data

    @classmethod
    def _parse_path_segments(cls, path: str) -> list:
        """Parse a JSONPath into segments."""
        if path == '$':
            return []

        # Remove leading $. or $
        if path.startswith('$.'):
            path = path[2:]
        elif path.startswith('$'):
            path = path[1:]

        segments = []
        current = ''
        i = 0

        while i < len(path):
            char = path[i]

            if char == '.':
                if current:
                    segments.append(current)
                    current = ''
            elif char == '[':
                if current:
                    segments.append(current)
                    current = ''
                # Find matching ]
                j = i + 1
                while j < len(path) and path[j] != ']':
                    j += 1
                bracket_content = path[i + 1:j]
                # Check if it's a number or quoted string
                if bracket_content.isdigit():
                    segments.append(int(bracket_content))
                elif bracket_content.startswith("'") and bracket_content.endswith("'"):
                    segments.append(bracket_content[1:-1])
                elif bracket_content.startswith('"') and bracket_content.endswith('"'):
                    segments.append(bracket_content[1:-1])
                else:
                    segments.append(bracket_content)
                i = j
            else:
                current += char

            i += 1

        if current:
            segments.append(current)

        return segments

    @classmethod
    def matches_pattern(cls, concrete_path: str, pattern: str) -> bool:
        """
        Check if a concrete path matches a JSONPath pattern.

        Supports:
        - Exact match: $.foo.bar
        - Recursive descent: $..field
        - Wildcard: $.items[*].name
        """
        # Handle recursive descent patterns
        if '..' in pattern:
            # Convert $..field to regex that matches any path ending with .field
            field = pattern.split('..')[-1]
            field_escaped = re.escape(field)
            regex = rf'.*\.{field_escaped}$|^\$\.{field_escaped}$'
            return bool(re.match(regex, concrete_path))

        # Handle wildcards
        if '[*]' in pattern or '.*' in pattern:
            # Convert to regex
            regex_pattern = pattern
            regex_pattern = regex_pattern.replace('.', r'\.')
            regex_pattern = regex_pattern.replace('[*]', r'\[\d+\]')
            regex_pattern = regex_pattern.replace('*', r'[^.]+')
            regex_pattern = f'^{regex_pattern}$'
            return bool(re.match(regex_pattern, concrete_path))

        # Exact match
        return concrete_path == pattern


def evaluate_condition(data: Any, condition: str) -> bool:
    """
    Evaluate a simple JSONPath condition.

    Supports:
    - $.field == 'value'
    - $.field != 'value'
    - $.field > value
    - $.field < value
    - $.field >= value
    - $.field <= value

    Args:
        data: The data to evaluate against
        condition: The condition string

    Returns:
        Boolean result of the condition
    """
    if not condition:
        return True

    # Parse the condition
    operators = ['==', '!=', '>=', '<=', '>', '<']
    operator = None
    for op in operators:
        if op in condition:
            operator = op
            break

    if not operator:
        return True

    parts = condition.split(operator, 1)
    if len(parts) != 2:
        return True

    path = parts[0].strip()
    expected = parts[1].strip()

    # Remove quotes from expected value
    if (expected.startswith("'") and expected.endswith("'")) or \
       (expected.startswith('"') and expected.endswith('"')):
        expected = expected[1:-1]
    elif expected.lower() == 'true':
        expected = True
    elif expected.lower() == 'false':
        expected = False
    elif expected.lower() == 'null':
        expected = None
    else:
        try:
            if '.' in expected:
                expected = float(expected)
            else:
                expected = int(expected)
        except ValueError:
            pass

    # Get actual value
    values = JSONPathMatcher.find_values(data, path)
    if not values:
        return False

    actual = values[0]

    # Compare
    try:
        if operator == '==':
            return actual == expected
        elif operator == '!=':
            return actual != expected
        elif operator == '>':
            return actual > expected
        elif operator == '<':
            return actual < expected
        elif operator == '>=':
            return actual >= expected
        elif operator == '<=':
            return actual <= expected
    except TypeError:
        return False

    return True

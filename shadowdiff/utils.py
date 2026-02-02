"""Utility functions for ShadowDiff engine."""

from __future__ import annotations

import re
import json
import copy
from datetime import timedelta
from typing import Any, Optional


def parse_duration(duration_str: str) -> timedelta:
    """
    Parse a duration string like '5s', '1m', '1h', '1d' into a timedelta.

    Args:
        duration_str: Duration string (e.g., '5s', '1m', '2h', '1d')

    Returns:
        timedelta object
    """
    if not duration_str:
        return timedelta(0)

    pattern = r'^(\d+(?:\.\d+)?)\s*([smhd])$'
    match = re.match(pattern, duration_str.strip().lower())

    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    value = float(match.group(1))
    unit = match.group(2)

    if unit == 's':
        return timedelta(seconds=value)
    elif unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)

    raise ValueError(f"Unknown duration unit: {unit}")


def deep_copy(obj: Any) -> Any:
    """Create a deep copy of an object."""
    return copy.deepcopy(obj)


def get_json_size_mb(obj: Any) -> float:
    """Get the approximate size of a JSON object in megabytes."""
    json_str = json.dumps(obj)
    return len(json_str.encode('utf-8')) / (1024 * 1024)


def is_numeric(value: Any) -> bool:
    """Check if a value is numeric (int or float)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def safe_cast(value: Any, cast_type: str) -> Any:
    """
    Safely cast a value to the specified type.

    Args:
        value: The value to cast
        cast_type: Target type ('int', 'float', 'string', 'boolean')

    Returns:
        The casted value
    """
    if value is None:
        return None

    try:
        if cast_type == 'int':
            return int(float(value))
        elif cast_type == 'float':
            return float(value)
        elif cast_type == 'string':
            return str(value)
        elif cast_type == 'boolean':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
    except (ValueError, TypeError):
        return value

    return value


def normalize_path(path: str) -> str:
    """Normalize a JSONPath expression."""
    if not path:
        return "$"
    if not path.startswith("$"):
        path = "$." + path
    return path


def build_path(parent_path: str, key: str | int) -> str:
    """Build a JSONPath from parent path and key."""
    if isinstance(key, int):
        return f"{parent_path}[{key}]"
    else:
        # Handle special characters in key names
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', str(key)):
            return f"{parent_path}.{key}"
        else:
            return f"{parent_path}['{key}']"


def get_type_name(value: Any) -> str:
    """Get a friendly type name for a value."""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "number"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return type(value).__name__


def values_equal(old: Any, new: Any) -> bool:
    """Check if two values are equal (handles type coercion for numbers)."""
    if type(old) == type(new):
        return old == new

    # Handle numeric comparison (int vs float)
    if is_numeric(old) and is_numeric(new):
        return float(old) == float(new)

    return old == new


def merge_dicts(base: dict, overlay: dict) -> dict:
    """
    Deep merge two dictionaries.
    Overlay values override base values.
    """
    result = deep_copy(base)

    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = deep_copy(value)

    return result


def extract_key_value(obj: dict, key_spec: str | list[str]) -> Optional[tuple]:
    """
    Extract key value(s) from an object based on key specification.

    Args:
        obj: The object to extract key from
        key_spec: Single key name or list of key names for composite key

    Returns:
        Tuple of key values or None if any key is missing
    """
    if not isinstance(obj, dict):
        return None

    if isinstance(key_spec, str):
        key_spec = [key_spec]

    values = []
    for key in key_spec:
        if key not in obj:
            return None
        values.append(obj[key])

    return tuple(values)


def format_key_value(key_value: tuple) -> str:
    """Format a key value tuple for display."""
    if len(key_value) == 1:
        return repr(key_value[0])
    return repr(key_value)

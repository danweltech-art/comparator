"""Comparison functions for different data types."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional
from functools import lru_cache

from .models import FieldRules
from .utils import parse_duration


# Cache for compiled regex patterns
@lru_cache(maxsize=256)
def _compile_pattern(pattern: str) -> re.Pattern:
    """Compile and cache a regex pattern."""
    return re.compile(pattern)


def compare_numbers(
    old: Any,
    new: Any,
    precision: Optional[float] = None
) -> tuple[bool, str]:
    """
    Compare two numeric values with optional precision tolerance.

    Args:
        old: The old value
        new: The new value
        precision: Maximum allowed difference

    Returns:
        Tuple of (is_match, message)
    """
    try:
        old_float = float(old)
        new_float = float(new)
    except (ValueError, TypeError) as e:
        return False, f"Cannot convert to number: {e}"

    if precision is not None:
        diff = abs(old_float - new_float)
        if diff <= precision:
            return True, ""
        return False, f"Value difference ({diff}) exceeds precision tolerance ({precision})"

    if old_float == new_float:
        return True, ""

    return False, f"Values differ: {old} != {new}"


def compare_strings(
    old: str,
    new: str,
    rules: FieldRules
) -> tuple[bool, str]:
    """
    Compare two string values with various rules.

    Args:
        old: The old string value
        new: The new string value
        rules: Field rules containing comparison options

    Returns:
        Tuple of (is_match, message)
    """
    old_str = str(old) if old is not None else ""
    new_str = str(new) if new is not None else ""

    # Apply transformations
    if rules.trim_whitespace:
        old_str = old_str.strip()
        new_str = new_str.strip()

    if rules.case_insensitive:
        old_str = old_str.lower()
        new_str = new_str.lower()

    # Pattern matching
    if rules.pattern:
        try:
            pattern = _compile_pattern(rules.pattern)
            old_matches = bool(pattern.match(old_str))
            new_matches = bool(pattern.match(new_str))

            if not old_matches and not new_matches:
                return False, f"Neither value matches pattern '{rules.pattern}'"
            if not old_matches:
                return False, f"Old value '{old}' doesn't match pattern '{rules.pattern}'"
            if not new_matches:
                return False, f"New value '{new}' doesn't match pattern '{rules.pattern}'"
            return True, ""
        except re.error as e:
            # Invalid regex, fall back to exact comparison
            pass

    # Exact comparison
    if old_str == new_str:
        return True, ""

    return False, f"Values differ: '{old}' != '{new}'"


def parse_datetime(value: str, fmt: Optional[str] = None) -> datetime:
    """
    Parse a datetime string using the specified format.

    Args:
        value: The datetime string
        fmt: Format string (or 'ISO8601' for ISO format)

    Returns:
        Parsed datetime object
    """
    if fmt is None or fmt.upper() == 'ISO8601':
        # Try ISO 8601 format
        # Handle various ISO formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%f%z',
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
        ]
        for f in formats:
            try:
                return datetime.strptime(value, f)
            except ValueError:
                continue

        # Try fromisoformat as fallback
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            pass

        raise ValueError(f"Cannot parse datetime '{value}' as ISO8601")
    else:
        return datetime.strptime(value, fmt)


def compare_datetime(
    old: str,
    new: str,
    fmt: Optional[str] = None,
    tolerance: Optional[str] = None
) -> tuple[bool, str]:
    """
    Compare two datetime values with optional tolerance.

    Args:
        old: The old datetime string
        new: The new datetime string
        fmt: Datetime format string
        tolerance: Tolerance duration string (e.g., '5s', '1m')

    Returns:
        Tuple of (is_match, message)
    """
    try:
        old_dt = parse_datetime(old, fmt)
        new_dt = parse_datetime(new, fmt)
    except ValueError as e:
        return False, f"Cannot parse datetime: {e}"

    if tolerance:
        try:
            tolerance_td = parse_duration(tolerance)
            diff = abs((old_dt - new_dt).total_seconds())
            if diff <= tolerance_td.total_seconds():
                return True, ""
            return False, f"Time difference ({diff}s) exceeds tolerance ({tolerance})"
        except ValueError as e:
            return False, f"Invalid tolerance format: {e}"

    if old_dt == new_dt:
        return True, ""

    return False, f"Datetimes differ: {old} != {new}"


def compare_with_rules(
    old: Any,
    new: Any,
    rules: FieldRules
) -> tuple[bool, str]:
    """
    Compare two values using the field rules.

    Args:
        old: The old value
        new: The new value
        rules: Field rules for comparison

    Returns:
        Tuple of (is_match, message)
    """
    # Apply type casting if specified
    if rules.cast:
        from .utils import safe_cast
        old = safe_cast(old, rules.cast.value)
        new = safe_cast(new, rules.cast.value)

    # Handle None/null values
    if old is None and new is None:
        return True, ""

    if old is None:
        if rules.has_default:
            old = rules.default
        else:
            return False, f"Old value is null, new value is '{new}'"

    if new is None:
        if rules.has_default:
            new = rules.default
        else:
            return False, f"Old value is '{old}', new value is null"

    # Datetime comparison
    if rules.datetime_format:
        return compare_datetime(
            str(old),
            str(new),
            rules.datetime_format,
            rules.datetime_tolerance
        )

    # Numeric comparison
    if rules.precision is not None:
        return compare_numbers(old, new, rules.precision)

    # Check if both are numeric (even without precision rule)
    if isinstance(old, (int, float)) and isinstance(new, (int, float)):
        return compare_numbers(old, new, None)

    # String comparison
    if isinstance(old, str) or isinstance(new, str):
        return compare_strings(old, new, rules)

    # Boolean comparison
    if isinstance(old, bool) and isinstance(new, bool):
        if old == new:
            return True, ""
        return False, f"Booleans differ: {old} != {new}"

    # Default comparison
    if old == new:
        return True, ""

    return False, f"Values differ: {old} != {new}"

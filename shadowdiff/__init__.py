"""
ShadowDiff - JSON Comparison Engine with OpenAPI Schema Extensions

A stateless service that compares two JSON payloads using OpenAPI schema
fragments with x-migration-* extensions to handle noise and determine
functional equivalence.
"""

from .engine import ShadowDiffEngine
from .models import (
    EngineConfig,
    DiffReport,
    DiffEntry,
    DiffType,
    Severity,
    WarningEntry,
)

__version__ = "2.0.0"
__all__ = [
    "ShadowDiffEngine",
    "EngineConfig",
    "DiffReport",
    "DiffEntry",
    "DiffType",
    "Severity",
    "WarningEntry",
]

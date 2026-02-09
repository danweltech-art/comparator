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
from .parent_validator import (
    ParentValidator,
    ParentConfig,
    ParentValidationReport,
    ParentValidationStatus,
)
from .extractor import (
    DataExtractor,
    ExtractConfig,
    ExtractedData,
    DataAggregator,
)
from .test_runner import (
    TestRunner,
    ScenarioResult,
    GlobalReport,
)
from .runner import (
    ShadowDiffRunner,
    run_tests,
)

__version__ = "2.1.0"
__all__ = [
    # Engine
    "ShadowDiffEngine",
    "EngineConfig",
    # Reports
    "DiffReport",
    "DiffEntry",
    "DiffType",
    "Severity",
    "WarningEntry",
    # Parent Validation
    "ParentValidator",
    "ParentConfig",
    "ParentValidationReport",
    "ParentValidationStatus",
    # Data Extraction
    "DataExtractor",
    "ExtractConfig",
    "ExtractedData",
    "DataAggregator",
    # Test Runner
    "TestRunner",
    "ScenarioResult",
    "GlobalReport",
    # Simple Runner
    "ShadowDiffRunner",
    "run_tests",
]

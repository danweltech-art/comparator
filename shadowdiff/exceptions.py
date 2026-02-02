"""Custom exceptions for ShadowDiff engine."""


class ShadowDiffError(Exception):
    """Base exception for ShadowDiff errors."""
    pass


class ValidationError(ShadowDiffError):
    """Raised when input validation fails."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class SchemaParseError(ShadowDiffError):
    """Raised when schema parsing fails."""
    def __init__(self, message: str, line: int = None, column: int = None, reason: str = None):
        super().__init__(message)
        self.message = message
        self.line = line
        self.column = column
        self.reason = reason


class ExternalRefError(ShadowDiffError):
    """Raised when an external $ref is encountered."""
    def __init__(self, ref: str):
        super().__init__(f"External $ref not allowed: {ref}")
        self.ref = ref


class MaxDepthExceededError(ShadowDiffError):
    """Raised when maximum recursion depth is exceeded."""
    def __init__(self, depth: int, path: str):
        super().__init__(f"Maximum depth ({depth}) exceeded at path: {path}")
        self.depth = depth
        self.path = path


class TimeoutError(ShadowDiffError):
    """Raised when processing timeout is exceeded."""
    def __init__(self, timeout_seconds: int):
        super().__init__(f"Processing timeout ({timeout_seconds}s) exceeded")
        self.timeout_seconds = timeout_seconds


class PayloadSizeError(ShadowDiffError):
    """Raised when payload size exceeds limit."""
    def __init__(self, size_mb: float, limit_mb: float):
        super().__init__(f"Payload size ({size_mb:.2f}MB) exceeds limit ({limit_mb}MB)")
        self.size_mb = size_mb
        self.limit_mb = limit_mb


class CircularRefError(ShadowDiffError):
    """Raised when a circular reference is detected in schema."""
    def __init__(self, path: str):
        super().__init__(f"Circular reference detected at: {path}")
        self.path = path


class RuleError(ShadowDiffError):
    """Raised when a migration rule is invalid."""
    def __init__(self, rule: str, message: str):
        super().__init__(f"Invalid rule '{rule}': {message}")
        self.rule = rule
        self.message = message

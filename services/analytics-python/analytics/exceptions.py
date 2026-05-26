"""Shared analytics exceptions."""


class OptionalDependencyError(RuntimeError):
    """Raised when an optional analytics dependency is needed but missing."""

    def __init__(self, package: str, purpose: str) -> None:
        super().__init__(
            f"Install '{package}' to use {purpose}. "
            "Run `pip install -e .` from services/analytics-python."
        )


class QuerySafetyError(ValueError):
    """Raised when a query violates analytics safety bounds."""

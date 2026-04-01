class ExploreError(RuntimeError):
    """Base exception for Sentry Explore failures."""


class ExploreAuthenticationError(ExploreError):
    """Raised when the Explore request is rejected due to authentication."""


class ExplorePermissionError(ExploreError):
    """Raised when the Explore request lacks the required permissions."""


class ExploreCapabilityError(ExploreError):
    """Raised when the Explore dataset or endpoint is unavailable."""


class ExploreQueryError(ExploreError):
    """Raised when an Explore request or payload is invalid."""

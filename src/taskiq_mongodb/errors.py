from taskiq.exceptions import ResultGetError


class ResultIsMissingError(ResultGetError):
    """Raised when a result is requested but none exists yet."""

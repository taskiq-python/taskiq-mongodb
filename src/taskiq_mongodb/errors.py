from taskiq.exceptions import ResultGetError, TaskiqError


class ResultIsMissingError(ResultGetError):
    """Raised when a result is requested but none exists yet."""


class UnknownQueueError(TaskiqError):
    """Raised when a message targets a queue that isn't configured on the broker."""

    __template__ = "Message references queue '{queue_name}' which is not configured on this broker."
    queue_name: str

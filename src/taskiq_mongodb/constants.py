from enum import StrEnum
from typing import Final


class MessageStatus(StrEnum):
    """Status of a queued message document in MongoBroker's collection."""

    PENDING = "pending"
    PROCESSING = "processing"
    DEAD = "dead"


DEFAULT_POLL_INTERVAL: Final[float] = 0.5
DEFAULT_VISIBILITY_TIMEOUT: Final[float] = 300
DEFAULT_MAX_RETRIES: Final[int] = 0

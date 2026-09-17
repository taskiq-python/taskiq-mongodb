from enum import StrEnum


class MessageStatus(StrEnum):
    """Status of a queued message document in MongoBroker's collection."""

    PENDING = "pending"
    PROCESSING = "processing"
    DEAD = "dead"

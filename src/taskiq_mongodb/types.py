from typing import NotRequired, TypedDict


class MongoQueue(TypedDict):
    """
    Per-queue configuration for MongoBroker.

    Attributes:
        name: Queue name; stored in each message's queue name field and matched against the queue name label set
            via some_task.kiq(...).with_labels(queue_name=...).
        poll_interval: Seconds to sleep between polls of this queue when it's empty. Defaults to 0.5.
        visibility_timeout: Seconds a claimed message from this queue may stay unacknowledged before it's
            returned to the queue. Defaults to 300.
        max_retries: Number of claims after which a repeatedly-unacknowledged message from this queue is moved
            to the "dead" status instead of being requeued. Defaults to 0, which dead-letters after the very
            first unacknowledged attempt; there is no dedicated "unlimited" value, pass a very large number
            instead.

    """

    name: str
    poll_interval: NotRequired[float]
    visibility_timeout: NotRequired[float]
    max_retries: NotRequired[int]

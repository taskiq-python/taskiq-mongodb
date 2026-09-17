import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from bson import ObjectId
from pymongo import AsyncMongoClient, ReturnDocument
from taskiq import AckableMessage, BrokerMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend

from taskiq_mongodb.constants import MessageStatus


_ReturnType = TypeVar("_ReturnType")


class MongoBroker(AsyncBroker):
    """TaskIQ broker that uses MongoDB as a queue."""

    def __init__(
        self,
        uri: str,
        db_name: str,
        collection_name: str = "taskiq_messages",
        queue_name: str = "taskiq",
        task_id_generator: Callable[[], str] | None = None,
        result_backend: AsyncResultBackend[_ReturnType] | None = None,
        poll_interval: float = 0.5,
        visibility_timeout: float = 300,
        max_retries: int | None = None,
    ) -> None:
        """
        Configure the MongoDB client and queue collection used to move tasks between processes.

        :param uri: MongoDB connection URI.
        :param db_name: Database name.
        :param collection_name: Collection used to store queued messages.
        :param queue_name: Logical queue name; lets several queues share one collection.
        :param task_id_generator: Custom task_id generator.
        :param result_backend: Custom result backend.
        :param poll_interval: Seconds to sleep between polls when the queue is empty.
        :param visibility_timeout: Seconds a claimed message may stay unacknowledged before it is returned to the queue.
        :param max_retries: Number of claims after which a repeatedly-unacknowledged message is moved to the ``dead``
            status instead of being requeued; None means it is requeued indefinitely.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.client = AsyncMongoClient(uri)
        self.db = self.client[db_name]
        self.col = self.db[collection_name]
        self.queue_name = queue_name
        self.poll_interval = poll_interval
        self.visibility_timeout = visibility_timeout
        self.max_retries = max_retries

    async def startup(self) -> None:
        """Create indexes used to claim pending messages and find stale ones."""
        await super().startup()
        await self.col.create_index([("queue_name", 1), ("status", 1), ("_id", 1)])
        await self.col.create_index([("queue_name", 1), ("status", 1), ("claimed_at", 1)])

    async def shutdown(self) -> None:
        """Close the MongoDB client."""
        await self.client.close()
        await super().shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Insert a pending message document into the queue collection.

        :param message: message to enqueue; ``labels["queue_name"]`` overrides the default queue.
        """
        queue_name = message.labels.get("queue_name") or self.queue_name
        await self.col.insert_one(
            {
                "queue_name": queue_name,
                "message": message.message,
                "status": MessageStatus.PENDING,
                "attempts": 0,
                "created_at": datetime.now(UTC),
                "claimed_at": None,
            },
        )

    async def _requeue_stale(self) -> None:
        """Return timed-out claims to pending, or mark them dead past max_retries."""
        threshold = datetime.now(UTC) - timedelta(seconds=self.visibility_timeout)
        stale_filter: dict[str, Any] = {
            "queue_name": self.queue_name,
            "status": MessageStatus.PROCESSING,
            "claimed_at": {"$lt": threshold},
        }
        if self.max_retries is not None:
            await self.col.update_many(
                {**stale_filter, "attempts": {"$gte": self.max_retries}},
                {"$set": {"status": MessageStatus.DEAD}},
            )
            stale_filter = {**stale_filter, "attempts": {"$lt": self.max_retries}}
        await self.col.update_many(
            stale_filter,
            {"$set": {"status": MessageStatus.PENDING, "claimed_at": None}},
        )

    def _ack_generator(self, message_id: ObjectId) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            await self.col.delete_one({"_id": message_id})

        return _ack

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Claim messages from the queue collection and yield them as they arrive.

        :yield: claimed messages; acknowledging one deletes its document.
        """
        while True:
            await self._requeue_stale()
            doc = await self.col.find_one_and_update(
                {"queue_name": self.queue_name, "status": MessageStatus.PENDING},
                {
                    "$set": {"status": MessageStatus.PROCESSING, "claimed_at": datetime.now(UTC)},
                    "$inc": {"attempts": 1},
                },
                sort=[("_id", 1)],
                return_document=ReturnDocument.AFTER,
            )
            if doc is None:
                await asyncio.sleep(self.poll_interval)
                continue
            yield AckableMessage(
                data=bytes(doc["message"]),
                ack=self._ack_generator(doc["_id"]),
            )

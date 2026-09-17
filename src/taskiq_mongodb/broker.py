import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar, cast

from bson import ObjectId
from pymongo import AsyncMongoClient, ReturnDocument
from taskiq import AckableMessage, BrokerMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend

from taskiq_mongodb.constants import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_VISIBILITY_TIMEOUT,
    MessageStatus,
)
from taskiq_mongodb.errors import UnknownQueueError
from taskiq_mongodb.types import MongoQueue


_ReturnType = TypeVar("_ReturnType")

_QueueItem = AckableMessage | BaseException


class MongoBroker(AsyncBroker):
    """
    TaskIQ broker that uses MongoDB as a queue.

    Messages are documents in a collection, discriminated by a queue name field. Workers claim the
    highest-priority, oldest pending document for a queue atomically (via MongoDB's find-and-update) and poll
    for new ones when that queue is empty, so no replica set or change streams are needed. A claimed document
    that is not acknowledged within its queue's visibility timeout (worker crashed or hung) is returned to the
    queue automatically, or moved to the "dead" status once the queue's retry limit is exceeded.

    Three message labels are understood, set via some_task.kiq(...).with_labels(...):

    - queue_name: routes the message to a queue other than the broker's default one.
    - priority: an integer; higher-priority messages within a queue are claimed first (default 0).
    - delay: seconds to wait before the message becomes claimable (default 0, claimable immediately).
    """

    def __init__(
        self,
        uri: str,
        database_name: str,
        queues: str | MongoQueue | Sequence[str | MongoQueue] = "taskiq",
        collection_name: str = "taskiq_messages",
        task_id_generator: Callable[[], str] | None = None,
        result_backend: AsyncResultBackend[_ReturnType] | None = None,
    ) -> None:
        """
        Configure the MongoDB client and queue collection used to move tasks between processes.

        :param uri: MongoDB connection URI.
        :param database_name: Database name.
        :param queues: A queue name, a single MongoQueue configuration, or a sequence of either (mixing plain
            names and configurations is fine) for multiqueue support.
        :param collection_name: Collection used to store queued messages; shared by every configured queue.
        :param task_id_generator: Custom task_id generator.
        :param result_backend: Custom result backend.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.client = AsyncMongoClient(uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

        self._queues = self._normalize_queues(queues)
        self._default_queue_name = self._queues[0]["name"]
        self._queues_by_name = {queue["name"]: queue for queue in self._queues}

    @property
    def default_queue_name(self) -> str:
        """Name of the queue used for messages that don't set the queue name label."""
        return self._default_queue_name

    @staticmethod
    def _normalize_queues(queues: str | MongoQueue | Sequence[str | MongoQueue]) -> list[MongoQueue]:
        if isinstance(queues, str):
            queue_list: list[MongoQueue] = [{"name": queues}]
        elif isinstance(queues, dict):
            queue_list = [cast("MongoQueue", queues)]
        else:
            sequence = cast("Sequence[str | MongoQueue]", queues)
            queue_list = [{"name": queue} if isinstance(queue, str) else queue for queue in sequence]

        if not queue_list:
            message = "At least one queue must be configured."
            raise ValueError(message)

        names = [queue["name"] for queue in queue_list]
        if len(names) != len(set(names)):
            message = "Queue names must be unique."
            raise ValueError(message)
        return queue_list

    async def startup(self) -> None:
        """Create indexes used to claim pending messages and find stale ones."""
        await super().startup()
        await self.collection.create_index(
            [("queue_name", 1), ("status", 1), ("priority", -1), ("_id", 1), ("visible_at", 1)],
        )
        await self.collection.create_index([("queue_name", 1), ("status", 1), ("claimed_at", 1)])

    async def shutdown(self) -> None:
        """Close the MongoDB client."""
        await self.client.close()
        await super().shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Insert a pending message document into the queue collection.

        :param message: message to enqueue; understands the queue_name, priority and delay labels.
        :raises UnknownQueueError: the queue_name label doesn't match a configured queue.
        """
        queue_name = message.labels.get("queue_name") or self._default_queue_name
        if queue_name not in self._queues_by_name:
            raise UnknownQueueError(queue_name=queue_name)

        now = datetime.now(UTC)
        delay = float(message.labels.get("delay", 0))
        await self.collection.insert_one(
            {
                "queue_name": queue_name,
                "message": message.message,
                "status": MessageStatus.PENDING,
                "attempts": 0,
                "priority": int(message.labels.get("priority", 0)),
                "created_at": now,
                "claimed_at": None,
                "visible_at": now + timedelta(seconds=delay) if delay else now,
            },
        )

    async def _requeue_stale_for_queue(self, queue: MongoQueue) -> None:
        """Return one queue's timed-out claims to pending, or mark them dead past its max_retries."""
        visibility_timeout = queue.get("visibility_timeout", DEFAULT_VISIBILITY_TIMEOUT)
        max_retries = queue.get("max_retries", DEFAULT_MAX_RETRIES)
        threshold = datetime.now(UTC) - timedelta(seconds=visibility_timeout)
        stale_filter: dict[str, Any] = {
            "queue_name": queue["name"],
            "status": MessageStatus.PROCESSING,
            "claimed_at": {"$lt": threshold},
        }
        await self.collection.update_many(
            {**stale_filter, "attempts": {"$gte": max_retries}},
            {"$set": {"status": MessageStatus.DEAD}},
        )
        await self.collection.update_many(
            {**stale_filter, "attempts": {"$lt": max_retries}},
            {"$set": {"status": MessageStatus.PENDING, "claimed_at": None}},
        )

    async def _requeue_stale(self) -> None:
        """Return timed-out claims to pending, or mark them dead, across every configured queue."""
        for queue in self._queues:
            await self._requeue_stale_for_queue(queue)

    def _ack_generator(self, message_id: ObjectId) -> Callable[[], Awaitable[None]]:
        async def _ack() -> None:
            await self.collection.delete_one({"_id": message_id})

        return _ack

    async def _poll_queue(self, queue: MongoQueue, incoming: "asyncio.Queue[_QueueItem]") -> None:
        """Continuously claim messages from a single queue and forward them to the shared incoming queue."""
        poll_interval = queue.get("poll_interval", DEFAULT_POLL_INTERVAL)
        try:
            while True:
                await self._requeue_stale_for_queue(queue)
                doc = await self.collection.find_one_and_update(
                    {
                        "queue_name": queue["name"],
                        "status": MessageStatus.PENDING,
                        "visible_at": {"$lte": datetime.now(UTC)},
                    },
                    {
                        "$set": {"status": MessageStatus.PROCESSING, "claimed_at": datetime.now(UTC)},
                        "$inc": {"attempts": 1},
                    },
                    sort=[("priority", -1), ("_id", 1)],
                    return_document=ReturnDocument.AFTER,
                )
                if doc is None:
                    await asyncio.sleep(poll_interval)
                    continue
                await incoming.put(
                    AckableMessage(data=bytes(doc["message"]), ack=self._ack_generator(doc["_id"])),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await incoming.put(exc)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Claim messages from every configured queue and yield them as they arrive.

        Each queue is polled by its own background task, so queues can have a different poll interval,
        visibility timeout, or retry limit without slowing each other down.

        :yield: claimed messages; acknowledging one deletes its document.
        """
        incoming: asyncio.Queue[_QueueItem] = asyncio.Queue()
        pollers = [asyncio.create_task(self._poll_queue(queue, incoming)) for queue in self._queues]
        try:
            while True:
                item = await incoming.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            for task in pollers:
                task.cancel()
            await asyncio.gather(*pollers, return_exceptions=True)

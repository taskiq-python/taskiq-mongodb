import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from taskiq import BrokerMessage
from taskiq.receiver import Receiver
from taskiq.utils import maybe_awaitable

from tests.conftest import MONGO_URI, TEST_VISIBILITY_TIMEOUT

from taskiq_mongodb import MongoBroker, MongoResultBackend, UnknownQueueError


def make_message(task_id: str = "task-1", payload: bytes = b"payload", **labels: Any) -> BrokerMessage:
    return BrokerMessage(task_id=task_id, task_name="some.task", message=payload, labels=labels)


async def test_kick_inserts_pending_message(broker: MongoBroker) -> None:
    await broker.kick(make_message())

    doc = await broker.collection.find_one({})

    assert doc is not None
    assert doc["status"] == "pending"
    assert doc["message"] == b"payload"
    assert doc["queue_name"] == broker.default_queue_name
    assert doc["attempts"] == 0
    assert doc["priority"] == 0
    assert doc["visible_at"] <= datetime.now(UTC).replace(tzinfo=None)


async def test_kick_to_unconfigured_queue_raises(broker: MongoBroker) -> None:
    with pytest.raises(UnknownQueueError):
        await broker.kick(make_message(queue_name="no-such-queue"))


async def test_listen_claims_message_and_marks_it_processing(broker: MongoBroker) -> None:
    await broker.kick(make_message())

    listener = broker.listen()
    try:
        ackable = await anext(listener)

        assert ackable.data == b"payload"
        doc = await broker.collection.find_one({})
        assert doc is not None
        assert doc["status"] == "processing"
        assert doc["attempts"] == 1
    finally:
        await listener.aclose()


async def test_ack_deletes_the_message(broker: MongoBroker) -> None:
    await broker.kick(make_message())

    listener = broker.listen()
    try:
        ackable = await anext(listener)
        await maybe_awaitable(ackable.ack())

        assert await broker.collection.count_documents({}) == 0
    finally:
        await listener.aclose()


async def test_listen_waits_for_new_messages_when_queue_is_empty(broker: MongoBroker) -> None:
    listener = broker.listen()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(listener), timeout=0.3)
    finally:
        await listener.aclose()


async def test_unacknowledged_message_is_returned_to_the_queue(broker: MongoBroker) -> None:
    await broker.kick(make_message())

    listener = broker.listen()
    try:
        await anext(listener)
    finally:
        await listener.aclose()

    past = datetime.now(UTC) - timedelta(seconds=TEST_VISIBILITY_TIMEOUT + 5)
    await broker.collection.update_many({}, {"$set": {"claimed_at": past}})

    await broker._requeue_stale()

    doc = await broker.collection.find_one({})
    assert doc is not None
    assert doc["status"] == "pending"
    assert doc["claimed_at"] is None


async def test_default_max_retries_is_zero_and_dead_letters_after_first_attempt(db_name: str) -> None:
    broker = MongoBroker(
        MONGO_URI,
        db_name,
        queues={"name": "taskiq", "poll_interval": 0.05, "visibility_timeout": TEST_VISIBILITY_TIMEOUT},
    )
    await broker.startup()
    try:
        await broker.kick(make_message())

        listener = broker.listen()
        try:
            await anext(listener)
        finally:
            await listener.aclose()

        past = datetime.now(UTC) - timedelta(seconds=TEST_VISIBILITY_TIMEOUT + 5)
        await broker.collection.update_many({}, {"$set": {"claimed_at": past}})

        await broker._requeue_stale()

        doc = await broker.collection.find_one({})
        assert doc is not None
        assert doc["status"] == "dead"
    finally:
        await broker.shutdown()


async def test_unacknowledged_message_is_marked_dead_after_max_retries(db_name: str) -> None:
    broker = MongoBroker(
        MONGO_URI,
        db_name,
        queues={
            "name": "taskiq",
            "poll_interval": 0.05,
            "visibility_timeout": TEST_VISIBILITY_TIMEOUT,
            "max_retries": 1,
        },
    )
    await broker.startup()
    try:
        await broker.kick(make_message())

        listener = broker.listen()
        try:
            await anext(listener)  # first (and only allowed) attempt
        finally:
            await listener.aclose()

        past = datetime.now(UTC) - timedelta(seconds=TEST_VISIBILITY_TIMEOUT + 5)
        await broker.collection.update_many({}, {"$set": {"claimed_at": past}})

        await broker._requeue_stale()

        doc = await broker.collection.find_one({})
        assert doc is not None
        assert doc["status"] == "dead"
    finally:
        await broker.shutdown()


async def test_priority_is_claimed_before_lower_priority_messages(broker: MongoBroker) -> None:
    await broker.kick(make_message(payload=b"low", priority=1))
    await broker.kick(make_message(payload=b"high", priority=10))

    listener = broker.listen()
    try:
        ackable = await anext(listener)
        assert ackable.data == b"high"
    finally:
        await listener.aclose()


async def test_delay_keeps_message_invisible_until_due(broker: MongoBroker) -> None:
    await broker.kick(make_message(delay=1))

    # Not claimable immediately: the claim query filters on visible_at <= now. A fresh listener is used for the
    # second wait, since asyncio.wait_for cancels (and so exhausts) the async generator it times out on.
    not_yet_listener = broker.listen()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(not_yet_listener), timeout=0.3)
    finally:
        await not_yet_listener.aclose()

    listener = broker.listen()
    try:
        ackable = await asyncio.wait_for(anext(listener), timeout=2)
        assert ackable.data == b"payload"
    finally:
        await listener.aclose()


async def test_listen_claims_from_every_configured_queue(db_name: str) -> None:
    broker = MongoBroker(
        MONGO_URI,
        db_name,
        queues=[
            {"name": "queue-a", "poll_interval": 0.05, "visibility_timeout": 5},
            {"name": "queue-b", "poll_interval": 0.05, "visibility_timeout": 5},
        ],
    )
    await broker.startup()
    try:
        await broker.kick(make_message(payload=b"from-a", queue_name="queue-a"))
        await broker.kick(make_message(payload=b"from-b", queue_name="queue-b"))

        listener = broker.listen()
        try:
            first = await asyncio.wait_for(anext(listener), timeout=2)
            second = await asyncio.wait_for(anext(listener), timeout=2)
            assert {first.data, second.data} == {b"from-a", b"from-b"}
        finally:
            await listener.aclose()
    finally:
        await broker.shutdown()


async def test_per_queue_visibility_timeout_and_max_retries_are_independent(db_name: str) -> None:
    broker = MongoBroker(
        MONGO_URI,
        db_name,
        queues=[
            {"name": "fast", "poll_interval": 0.05, "visibility_timeout": 1, "max_retries": 1},
            {"name": "slow", "poll_interval": 0.05, "visibility_timeout": 100, "max_retries": 1},
        ],
    )
    await broker.startup()
    try:
        await broker.kick(make_message(queue_name="fast"))
        await broker.kick(make_message(queue_name="slow"))

        listener = broker.listen()
        try:
            await asyncio.wait_for(anext(listener), timeout=2)
            await asyncio.wait_for(anext(listener), timeout=2)
        finally:
            await listener.aclose()

        past = datetime.now(UTC) - timedelta(seconds=2)
        await broker.collection.update_many({}, {"$set": {"claimed_at": past}})
        await broker._requeue_stale()

        fast_doc = await broker.collection.find_one({"queue_name": "fast"})
        slow_doc = await broker.collection.find_one({"queue_name": "slow"})
        assert fast_doc is not None
        assert slow_doc is not None
        assert fast_doc["status"] == "dead"  # 2s stale > fast's 1s visibility_timeout
        assert slow_doc["status"] == "processing"  # 2s stale < slow's 100s visibility_timeout
    finally:
        await broker.shutdown()


async def test_end_to_end_task_execution(db_name: str) -> None:
    result_backend = MongoResultBackend(MONGO_URI, db_name)
    broker = MongoBroker(
        MONGO_URI,
        db_name,
        queues={"name": "taskiq", "poll_interval": 0.05, "visibility_timeout": 5},
    ).with_result_backend(result_backend)

    @broker.task
    async def add_one(value: int) -> int:
        return value + 1

    await broker.startup()
    try:
        task = await add_one.kiq(41)

        listener = broker.listen()
        try:
            message = await anext(listener)
            await Receiver(broker=broker).callback(message=message)
        finally:
            await listener.aclose()

        result = await result_backend.get_result(task.task_id)

        assert result.return_value == 42
        assert result.is_err is False
        assert await broker.collection.count_documents({}) == 0
    finally:
        await broker.shutdown()

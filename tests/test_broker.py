import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from taskiq import BrokerMessage
from taskiq.receiver import Receiver
from taskiq.utils import maybe_awaitable

from tests.conftest import MONGO_URI

from taskiq_mongodb import MongoBroker, MongoResultBackend


def make_message(task_id: str = "task-1", **labels: str) -> BrokerMessage:
    return BrokerMessage(task_id=task_id, task_name="some.task", message=b"payload", labels=labels)


async def test_kick_inserts_pending_message(broker: MongoBroker) -> None:
    await broker.kick(make_message())

    doc = await broker.col.find_one({})

    assert doc is not None
    assert doc["status"] == "pending"
    assert doc["message"] == b"payload"
    assert doc["queue_name"] == broker.queue_name
    assert doc["attempts"] == 0


async def test_kick_respects_queue_name_label(broker: MongoBroker) -> None:
    await broker.kick(make_message(queue_name="other-queue"))

    doc = await broker.col.find_one({})
    assert doc is not None
    assert doc["queue_name"] == "other-queue"

    listener = broker.listen()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(listener), timeout=0.3)
    finally:
        await listener.aclose()


async def test_listen_claims_message_and_marks_it_processing(broker: MongoBroker) -> None:
    await broker.kick(make_message())

    listener = broker.listen()
    try:
        ackable = await anext(listener)

        assert ackable.data == b"payload"
        doc = await broker.col.find_one({})
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

        assert await broker.col.count_documents({}) == 0
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

    past = datetime.now(UTC) - timedelta(seconds=broker.visibility_timeout + 5)
    await broker.col.update_many({}, {"$set": {"claimed_at": past}})

    await broker._requeue_stale()

    doc = await broker.col.find_one({})
    assert doc is not None
    assert doc["status"] == "pending"
    assert doc["claimed_at"] is None


async def test_unacknowledged_message_is_marked_dead_after_max_retries(db_name: str) -> None:
    broker = MongoBroker(MONGO_URI, db_name, poll_interval=0.05, visibility_timeout=1, max_retries=1)
    await broker.startup()
    try:
        await broker.kick(make_message())

        listener = broker.listen()
        try:
            await anext(listener)  # first (and only allowed) attempt
        finally:
            await listener.aclose()

        past = datetime.now(UTC) - timedelta(seconds=broker.visibility_timeout + 5)
        await broker.col.update_many({}, {"$set": {"claimed_at": past}})

        await broker._requeue_stale()

        doc = await broker.col.find_one({})
        assert doc is not None
        assert doc["status"] == "dead"
    finally:
        await broker.shutdown()


async def test_end_to_end_task_execution(db_name: str) -> None:
    result_backend = MongoResultBackend(MONGO_URI, db_name)
    broker = MongoBroker(
        MONGO_URI,
        db_name,
        poll_interval=0.05,
        visibility_timeout=5,
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
        assert await broker.col.count_documents({}) == 0
    finally:
        await broker.shutdown()

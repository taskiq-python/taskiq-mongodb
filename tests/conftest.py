import os
import uuid
from collections.abc import AsyncIterator

import pytest_asyncio
from pymongo import AsyncMongoClient

from taskiq_mongodb import MongoBroker, MongoResultBackend


MONGO_URI = os.environ.get("TASKIQ_MONGODB_TEST_URI", "mongodb://root:password@localhost:27017")
TEST_VISIBILITY_TIMEOUT = 1


@pytest_asyncio.fixture
async def db_name() -> AsyncIterator[str]:
    name = f"taskiq_mongodb_test_{uuid.uuid4().hex}"
    yield name
    client: AsyncMongoClient = AsyncMongoClient(MONGO_URI)
    await client.drop_database(name)
    await client.close()


@pytest_asyncio.fixture
async def result_backend(db_name: str) -> AsyncIterator[MongoResultBackend]:
    backend = MongoResultBackend(MONGO_URI, db_name)
    await backend.startup()
    yield backend
    await backend.shutdown()


@pytest_asyncio.fixture
async def broker(db_name: str) -> AsyncIterator[MongoBroker]:
    instance = MongoBroker(
        MONGO_URI,
        db_name,
        queues={
            "name": "taskiq",
            "poll_interval": 0.05,
            "visibility_timeout": TEST_VISIBILITY_TIMEOUT,
            "max_retries": 5,
        },
    )
    await instance.startup()
    yield instance
    await instance.shutdown()

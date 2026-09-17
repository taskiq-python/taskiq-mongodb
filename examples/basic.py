"""
Basic example of using TaskIQ with the MongoDB broker and result backend.

Requires a MongoDB instance, e.g. `make run_infra` from the repo root.

How to run:
    1. Run worker: taskiq worker examples.basic:broker -w 1
    2. Run client: uv run examples/basic.py
"""

import asyncio
import os

from taskiq_mongodb import MongoBroker, MongoResultBackend


MONGO_URI = os.environ.get("TASKIQ_MONGODB_URI", "mongodb://root:password@localhost:27017")

broker = MongoBroker(MONGO_URI, "taskiq_example").with_result_backend(
    MongoResultBackend(MONGO_URI, "taskiq_example"),
)


@broker.task
async def add_one(value: int) -> int:
    return value + 1


async def main() -> None:
    await broker.startup()
    # Send the task to the broker.
    task = await add_one.kiq(1)
    # Wait for the result.
    result = await task.wait_result(timeout=5)
    print(f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        print(f"Returned value: {result.return_value}")
    else:
        print("Error found while executing task.")
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

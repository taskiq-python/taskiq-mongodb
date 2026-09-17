"""
Example showing how MongoBroker requeues and eventually dead-letters
messages that are claimed but never acknowledged (e.g. a worker crashed
mid-task).

Requires a MongoDB instance, e.g. `make run_infra` from the repo root.
This script drives the broker directly, without a `taskiq worker` process,
so it can control acknowledgement itself.

How to run:
    uv run examples/dead_letter.py
"""

import asyncio
import contextlib
import os

from taskiq import BrokerMessage

from taskiq_mongodb import MongoBroker


MONGO_URI = os.environ.get("TASKIQ_MONGODB_URI", "mongodb://root:password@localhost:27017")


async def main() -> None:
    # Small visibility_timeout/poll_interval and max_retries=2 so the example
    # finishes quickly; production values would be minutes, not seconds.
    broker = MongoBroker(
        MONGO_URI,
        "taskiq_example",
        poll_interval=0.2,
        visibility_timeout=1,
        max_retries=2,
    )
    await broker.startup()

    await broker.kick(
        BrokerMessage(task_id="demo", task_name="demo:task", message=b"payload", labels={}),
    )

    listener = broker.listen()
    for attempt in range(1, broker.max_retries + 1):
        await anext(listener)
        print(f"attempt {attempt}: claimed message, deliberately not acknowledging it")
        # Simulate a crashed worker: never call `await message.ack()`.
        await asyncio.sleep(broker.visibility_timeout + 0.5)  # let the claim go stale

    # One more poll cycle is what actually notices attempts >= max_retries
    # and flips the message to "dead"; nothing is left to claim afterwards.
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(anext(listener), timeout=1)

    doc = await broker.col.find_one({})
    print(f"final status after {broker.max_retries} unacknowledged attempts: {doc['status']}")

    await listener.aclose()
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

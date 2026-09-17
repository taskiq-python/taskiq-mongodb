"""
Example of automatic retries with MongoBroker and TaskIQ's SimpleRetryMiddleware.

A task that raises an exception is re-kicked by the middleware, up to the configured retry limit. A task that
keeps failing exhausts its retry budget and taskiq gives up on it (the application-level analogue of a message
landing in a dead-letter queue).

Requires a MongoDB instance, e.g. `make run_infra` from the repo root.

How to run:
    1. Run worker: taskiq worker examples.dead_letter:broker -w 1
    2. Run client: uv run examples/dead_letter.py
"""

import asyncio
import os

from taskiq import Context, TaskiqDepends
from taskiq.middlewares import SimpleRetryMiddleware

from taskiq_mongodb import MongoBroker, MongoResultBackend


MONGO_URI = os.environ.get("TASKIQ_MONGODB_URI", "mongodb://root:password@localhost:27017")

broker = (
    MongoBroker(MONGO_URI, "taskiq_example")
    .with_result_backend(MongoResultBackend(MONGO_URI, "taskiq_example"))
    .with_middlewares(SimpleRetryMiddleware(default_retry_count=3))
)


@broker.task(retry_on_error=True, max_retries=3)
async def flaky_task(fail_times: int, context: Context = TaskiqDepends()) -> str:
    attempt = int(context.message.labels.get("_retries", 0)) + 1
    if attempt <= fail_times:
        message = f"attempt {attempt} failed on purpose ({fail_times} failures configured)"
        raise RuntimeError(message)
    return f"succeeded on attempt {attempt}"


async def main() -> None:
    await broker.startup()

    # Fails twice, then succeeds within the max_retries=3 budget.
    recovers = await flaky_task.kiq(fail_times=2)
    result = await recovers.wait_result(timeout=10)
    print(f"recovers after retries: is_err={result.is_err} value={result.return_value!r}")

    # Fails more times than max_retries allows: retries are exhausted and taskiq gives up on the task, same idea
    # as dead-lettering a message.
    gives_up = await flaky_task.kiq(fail_times=10)
    result = await gives_up.wait_result(timeout=10)
    print(f"exhausts retries: is_err={result.is_err}")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

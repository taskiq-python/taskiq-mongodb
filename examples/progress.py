"""
Example of task progress reporting: a task updates its own progress via ProgressTracker while it runs, and the client
polls it through MongoResultBackend while the worker is still executing the task.

Requires a MongoDB instance, e.g. `make run_infra` from the repo root.

How to run:
    1. Run worker: taskiq worker examples.progress:broker -w 1
    2. Run client: uv run examples/progress.py
"""

import asyncio
import os

from taskiq import TaskiqDepends
from taskiq.depends.progress_tracker import ProgressTracker, TaskState

from taskiq_mongodb import MongoBroker, MongoResultBackend


MONGO_URI = os.environ.get("TASKIQ_MONGODB_URI", "mongodb://root:password@localhost:27017")

broker = MongoBroker(MONGO_URI, "taskiq_example").with_result_backend(
    MongoResultBackend(MONGO_URI, "taskiq_example"),
)


@broker.task
async def process_batch(total: int, tracker: ProgressTracker[int] = TaskiqDepends()) -> str:
    for done in range(1, total + 1):
        await asyncio.sleep(0.5)
        await tracker.set_progress(TaskState.STARTED, meta=done)
    return f"processed {total} items"


async def main() -> None:
    await broker.startup()

    task = await process_batch.kiq(total=5)

    last_meta = None
    while not await task.is_ready():
        progress = await task.get_progress()
        if progress is not None and progress.meta != last_meta:
            last_meta = progress.meta
            print(f"progress: state={progress.state} meta={progress.meta}")
        await asyncio.sleep(0.2)

    result = await task.get_result()
    print(f"result: {result.return_value}")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

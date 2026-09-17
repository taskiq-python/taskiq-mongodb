"""
Example of using MongoResultBackend directly, without a broker or worker.

Useful when you just need durable storage for task-like results/progress
computed elsewhere.

Requires a MongoDB instance, e.g. `make run_infra` from the repo root.

How to run:
    uv run examples/result_backend_only.py
"""

import asyncio
import os

from taskiq import TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress, TaskState

from taskiq_mongodb import MongoResultBackend


MONGO_URI = os.environ.get("TASKIQ_MONGODB_URI", "mongodb://root:password@localhost:27017")


async def main() -> None:
    backend = MongoResultBackend(MONGO_URI, "taskiq_example")
    await backend.startup()

    task_id = "example-task-1"

    await backend.set_progress(task_id, TaskProgress(state=TaskState.STARTED, meta={"step": 1}))
    print("progress:", await backend.get_progress(task_id))

    await backend.set_result(
        task_id,
        TaskiqResult(is_err=False, return_value=42, execution_time=0.05),
    )
    print("is ready:", await backend.is_result_ready(task_id))

    result = await backend.get_result(task_id)
    print("return value:", result.return_value)

    await backend.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

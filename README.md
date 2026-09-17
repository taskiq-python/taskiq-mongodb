# taskiq-mongodb

MongoDB broker and result backend for [TaskIQ](https://github.com/taskiq-python/taskiq).

## Installation

```bash
pip install taskiq-mongodb
```

Requires Python 3.11+ and a MongoDB instance (a standalone `mongod` is enough — no replica set needed).

## Basic example

```python
# broker.py
import asyncio
import os

from taskiq_mongodb import MongoBroker, MongoResultBackend

MONGO_URI = os.environ.get("TASKIQ_MONGODB_URI", "mongodb://root:password@localhost:27017")

broker = MongoBroker(MONGO_URI, "my_app").with_result_backend(
    MongoResultBackend(MONGO_URI, "my_app"),
)


@broker.task
async def add_one(value: int) -> int:
    return value + 1


async def main() -> None:
    await broker.startup()
    task = await add_one.kiq(1)
    result = await task.wait_result(timeout=5)
    print(result.return_value)  # 2
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

Run a worker in one process and the script above in another:

```bash
taskiq worker broker:broker -w 1
python broker.py
```

More runnable examples live in [`examples/`](examples/).

## Result backend

`MongoResultBackend` stores task results (and progress, see below) as documents in a collection, one per task id.

```python
from taskiq_mongodb import MongoResultBackend

result_backend = MongoResultBackend(
    "mongodb://root:password@localhost:27017",  # pragma: allowlist secret
    "my_app",
    collection_name="task_results",  # default
    keep_results=True,               # keep the document after get_result reads it
    ttl_seconds=3600,                # auto-expire results after an hour; 0 disables expiry
)
```

It works standalone too, without a broker — useful when you just need durable storage for results or progress computed
elsewhere. See [`examples/progress.py`](examples/progress.py).

## Broker

`MongoBroker` distributes tasks between worker processes. A worker claims the oldest pending message for a queue with
an atomic `find_one_and_update`, and polls again when the queue is empty. A claimed message that isn't acknowledged
within its queue's visibility timeout (worker crashed or hung) is returned to the queue automatically, or moved to the
"dead" status once the queue's retry limit is exceeded.

```python
from taskiq_mongodb import MongoBroker

broker = MongoBroker(
    "mongodb://root:password@localhost:27017",  # pragma: allowlist secret
    "my_app",
    queues="taskiq",              # default; see Multiple queues below
    collection_name="taskiq_messages",  # default
)
```

Acknowledging a message deletes its document, so a healthy queue collection stays small.

### Multiple queues

Pass a queue name, a single queue configuration, or a sequence of either to `queues=`. Each queue is polled by its own
background task, so different queues can have different poll intervals, visibility timeouts and retry limits without
affecting each other:

```python
from taskiq_mongodb import MongoBroker, MongoQueue

broker = MongoBroker(
    "mongodb://root:password@localhost:27017",  # pragma: allowlist secret
    "my_app",
    queues=[
        "default",
        MongoQueue(name="critical", poll_interval=0.1, visibility_timeout=30),
        MongoQueue(name="reports", visibility_timeout=600, max_retries=3),
    ],
)
```

The first queue (`"default"` above) is used for tasks that don't say otherwise. Route a task to a different queue with
the `queue_name` label:

```python
await add_one.kicker().with_labels(queue_name="critical").kiq(1)
```

Kicking a task to a queue that isn't configured on the broker raises `UnknownQueueError` (from `broker.kick()` directly;
through `.kiq()`, as above, TaskIQ wraps it in `SendTaskError` with `UnknownQueueError` as the cause).

### Priority

Set the `priority` label (an integer, default 0) to have a task claimed before lower-priority ones waiting in the same
queue:

```python
await add_one.kicker().with_labels(priority=10).kiq(1)
```

### Delayed messages

Set the `delay` label (seconds) to make a task claimable only after that delay has passed:

```python
await add_one.kicker().with_labels(delay=30).kiq(1)  # claimable in 30 seconds
```

### Retries and dead-lettering

`MongoBroker` has two independent, complementary retry mechanisms:

- **Crash recovery** — every queue has a `visibility_timeout` (default 300s) and `max_retries` (default 0).
  If a worker claims a message and crashes before acknowledging it, the message becomes claimable again once the timeout
  passes. After `max_retries` such claims it's moved to the "dead" status instead of being requeued. `max_retries=0`
  (the default) dead-letters after the very first unacknowledged attempt; there's no dedicated "unlimited" value — pass
  a very large number instead.
- **Application-level retries** — for a task that raises an exception (as opposed to a worker that crashes),
  use TaskIQ's own `SimpleRetryMiddleware` to re-kick it a bounded number of times:

```python
from taskiq.middlewares import SimpleRetryMiddleware

broker = broker.with_middlewares(SimpleRetryMiddleware(default_retry_count=3))


@broker.task(retry_on_error=True, max_retries=3)
async def flaky_task() -> None: ...
```

See [`examples/dead_letter.py`](examples/dead_letter.py) for a full runnable example of the second kind.

## Progress reporting

Tasks can report their own progress while running, using TaskIQ's `ProgressTracker`, and a caller can poll it
through the result backend while the task is still executing:

```python
from taskiq import TaskiqDepends
from taskiq.depends.progress_tracker import ProgressTracker, TaskState


@broker.task
async def process_batch(total: int, tracker: ProgressTracker[int] = TaskiqDepends()) -> str:
    for done in range(1, total + 1):
        ...
        await tracker.set_progress(TaskState.STARTED, meta=done)
    return f"processed {total} items"


task = await process_batch.kiq(total=5)
progress = await task.get_progress()  # -> TaskProgress(state=..., meta=...) or None
```

See [`examples/progress.py`](examples/progress.py) for the full picture, including the polling loop.

## License

MIT — see [LICENSE](LICENSE).

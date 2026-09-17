import pytest
from taskiq import TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress, TaskState

from tests.conftest import MONGO_URI

from taskiq_mongodb import MongoResultBackend, ResultIsMissingError


def make_result(return_value: int = 42) -> TaskiqResult:
    return TaskiqResult(is_err=False, return_value=return_value, execution_time=0.1, log="some log")


async def test_set_and_get_result(result_backend: MongoResultBackend) -> None:
    await result_backend.set_result("task-1", make_result())

    result = await result_backend.get_result("task-1", with_logs=True)

    assert result.return_value == 42
    assert result.is_err is False
    assert result.log == "some log"


async def test_get_result_without_logs_clears_log(result_backend: MongoResultBackend) -> None:
    await result_backend.set_result("task-1", make_result())

    result = await result_backend.get_result("task-1", with_logs=False)

    assert result.log is None


async def test_get_result_raises_when_missing(result_backend: MongoResultBackend) -> None:
    with pytest.raises(ResultIsMissingError):
        await result_backend.get_result("missing-task")


async def test_is_result_ready(result_backend: MongoResultBackend) -> None:
    assert await result_backend.is_result_ready("task-1") is False

    await result_backend.set_result("task-1", make_result())

    assert await result_backend.is_result_ready("task-1") is True


async def test_progress_only_document_is_not_a_ready_result(result_backend: MongoResultBackend) -> None:
    await result_backend.set_progress("task-1", TaskProgress(state=TaskState.STARTED, meta=None))

    assert await result_backend.is_result_ready("task-1") is False


async def test_keep_results_false_deletes_after_get(db_name: str) -> None:
    backend = MongoResultBackend(
        MONGO_URI,
        db_name,
        keep_results=False,
    )
    await backend.startup()
    try:
        await backend.set_result("task-1", make_result())

        await backend.get_result("task-1")

        assert await backend.is_result_ready("task-1") is False
    finally:
        await backend.shutdown()


async def test_ttl_seconds_zero_omits_expire_at(db_name: str) -> None:
    backend = MongoResultBackend(
        MONGO_URI,
        db_name,
        ttl_seconds=0,
    )
    await backend.startup()
    try:
        await backend.set_result("task-1", make_result())

        doc = await backend.col.find_one({"task_id": "task-1"})

        assert doc is not None
        assert "expire_at" not in doc
    finally:
        await backend.shutdown()


async def test_ttl_seconds_positive_sets_expire_at(result_backend: MongoResultBackend) -> None:
    await result_backend.set_result("task-1", make_result())

    doc = await result_backend.col.find_one({"task_id": "task-1"})

    assert doc is not None
    assert "expire_at" in doc


async def test_set_and_get_progress(result_backend: MongoResultBackend) -> None:
    await result_backend.set_progress("task-1", TaskProgress(state=TaskState.STARTED, meta={"step": 1}))

    progress = await result_backend.get_progress("task-1")

    assert progress is not None
    assert progress.state == TaskState.STARTED
    assert progress.meta == {"step": 1}


async def test_get_progress_returns_none_when_missing(result_backend: MongoResultBackend) -> None:
    assert await result_backend.get_progress("missing-task") is None


async def test_startup_creates_expected_indexes(result_backend: MongoResultBackend) -> None:
    indexes = await result_backend.col.index_information()

    assert "task_id_1" in indexes
    assert "expire_at_1" in indexes

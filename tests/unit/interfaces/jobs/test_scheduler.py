import queue
from unittest.mock import patch

from mindsdb.interfaces.jobs import scheduler as scheduler_module


def test_execute_async_marks_failure_on_exception():
    q_in = queue.Queue()
    q_out = queue.Queue()
    q_in.put({"type": "task", "record_id": 1, "history_id": 1})
    q_in.put({"type": "exit"})

    with (
        patch.object(scheduler_module, "JobsExecutor") as jobs_executor_cls,
        patch.object(scheduler_module.db.session, "rollback") as rollback_mock,
    ):
        jobs_executor_cls.return_value.execute_task_local.side_effect = RuntimeError("boom")
        scheduler_module.execute_async(q_in, q_out)

    assert q_out.get_nowait() is False
    rollback_mock.assert_called_once()

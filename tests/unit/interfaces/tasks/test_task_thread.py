from unittest.mock import patch

from mindsdb.interfaces.tasks.task_thread import TaskThread


def test_run_returns_when_task_record_is_missing():
    thread = TaskThread(task_id=123)

    with (
        patch("mindsdb.interfaces.tasks.task_thread.db.Tasks.query.get", return_value=None) as get_mock,
        patch("mindsdb.interfaces.tasks.task_thread.ctx.set_default") as set_default_mock,
        patch("mindsdb.interfaces.tasks.task_thread.db.session.commit") as commit_mock,
    ):
        thread.run()

    get_mock.assert_called_once_with(123)
    set_default_mock.assert_not_called()
    commit_mock.assert_not_called()

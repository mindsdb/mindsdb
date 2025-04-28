import datetime as dt
import os
import socket
import time
from threading import Event

import sqlalchemy as sa

from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from .task_thread import TaskThread

logger = log.getLogger(__name__)


class TaskMonitor:

    MONITOR_INTERVAL_SECONDS = 2
    LOCK_EXPIRED_SECONDS = MONITOR_INTERVAL_SECONDS * 30

    def __init__(self):
        self._active_tasks = {}

    def start(self, stop_event: Event = None):
        config = Config()
        db.init()
        self.config = config

        while True:
            try:
                self.check_tasks()

                db.session.rollback()  # disable cache
                time.sleep(self.MONITOR_INTERVAL_SECONDS)

            except (SystemExit, KeyboardInterrupt):
                self.stop_all_tasks()
                return

            except Exception as e:
                logger.error(e)
                db.session.rollback()

            if stop_event is not None and stop_event.is_set():
                return

    def stop_all_tasks(self):

        active_tasks = list(self._active_tasks.keys())
        for task_id in active_tasks:
            self.stop_task(task_id)

    def check_tasks(self):
        allowed_tasks = set()

        for task in db.session.query(db.Tasks).filter(db.Tasks.active == True):  # noqa
            allowed_tasks.add(task.id)

            # start new tasks
            if task.id not in self._active_tasks:
                self.start_task(task)

        # Check active tasks
        active_tasks = list(self._active_tasks.items())
        for task_id, task in active_tasks:

            if task_id not in allowed_tasks:
                # old task
                self.stop_task(task_id)

            elif not task.is_alive():
                # dead task
                self.stop_task(task_id)

            else:
                # need to be reloaded ?
                record = db.Tasks.query.get(task_id)
                if record.reload:
                    record.reload = False
                    self.stop_task(task_id)
                else:
                    # set alive time of running tasks
                    self._set_alive(task_id)

    def _lock_task(self, task):
        run_by = f"{socket.gethostname()} {os.getpid()}"
        db_date = db.session.query(sa.func.current_timestamp()).first()[0]
        if task.run_by == run_by:
            # already locked
            task.alive_time = db_date

        elif task.alive_time is None:
            # not locked yet
            task.run_by = run_by
            task.alive_time = db_date

        elif db_date - task.alive_time > dt.timedelta(
            seconds=self.LOCK_EXPIRED_SECONDS
        ):
            # lock expired
            task.run_by = run_by
            task.alive_time = db_date

        else:
            return False

        db.session.commit()
        return True

    def _set_alive(self, task_id):
        db_date = db.session.query(sa.func.current_timestamp()).first()[0]
        task = db.Tasks.query.get(task_id)
        task.alive_time = db_date
        db.session.commit()

    def _unlock_task(self, task_id):
        task = db.Tasks.query.get(task_id)
        if task is not None:
            task.alive_time = None
            db.session.commit()

    def start_task(self, task):
        if not self._lock_task(task):
            # can't lock, skip
            return

        thread = TaskThread(task.id)

        thread.start()
        self._active_tasks[task.id] = thread

    def stop_task(self, task_id: int):
        thread = self._active_tasks[task_id]
        thread.stop()
        thread.join(1)

        if thread.is_alive():
            # don't delete task, wait next circle
            return

        del self._active_tasks[task_id]
        self._unlock_task(task_id)


def start(verbose=False):

    monitor = TaskMonitor()
    monitor.start()


if __name__ == "__main__":
    start()

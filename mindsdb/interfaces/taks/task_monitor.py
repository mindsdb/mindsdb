import time
import socket
import os
import datetime as dt

from mindsdb.utilities import log

from mindsdb.utilities.log import initialize_log

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db

from .task_thread import TaskThread


class TaskMonitor:

    _MONITOR_INTERVAL_SECONDS = 1

    def __init__(self):
        self._active_tasks = {}

    def start(self):
        config = Config()
        db.init()
        initialize_log(config, 'jobs', wrap_print=True)
        self.config = config

        while True:
            try:
                self.check_tasks()

            except (SystemExit, KeyboardInterrupt):
                self.stop_all_tasks()
                raise
            except Exception as e:
                log.logger.error(e)

            db.session.rollback()  # disable cache
            time.sleep(self._MONITOR_INTERVAL_SECONDS)

    def stop_all_tasks(self):

        active_tasks = list(self._active_tasks.keys())
        for task_id in active_tasks:
            self.stop_task(task_id)

    def check_tasks(self):
        allowed_tasks = set()

        for task in db.Tasks.query\
                .filter(db.Tasks.active == True)\
                .all():
            allowed_tasks.add(task.id)

            # start new tasks
            if task.id not in self._active_tasks:
                self.start_task(task)

        # Check old tasks to stop
        active_tasks = list(self._active_tasks.keys())
        for task_id in active_tasks:
            if task_id not in allowed_tasks:
                self.stop_task(task_id)

        # check dead tasks
        active_tasks = list(self._active_tasks.items())
        for task_id, task in active_tasks:
            if not task.is_alive():
                self.stop_task(task_id)

        # set alive time to running tasks
        for task_id in self._active_tasks.keys():
            self._set_alive(task_id)

    def _lock_task(self, task):
        run_by = f'{socket.gethostname()} {os.getpid()}'
        if task.run_by == run_by:
            # already locked
            task.alive_time = dt.datetime.now()

        elif task.alive_time is None:
            # not locked yet
            task.run_by = run_by
            task.alive_time = dt.datetime.now()

        elif dt.datetime.now() - task.alive_time > dt.timedelta(seconds=self._MONITOR_INTERVAL_SECONDS * 10):
            # lock expired
            task.run_by = run_by
            task.alive_time = dt.datetime.now()

        else:
            return False

        db.session.commit()
        return True

    def _set_alive(self, task_id):
        task = db.Tasks.query.get(task_id)
        task.alive_time = dt.datetime.now()
        db.session.commit()

    def _unlock_task(self, task_id):
        task = db.Tasks.query.get(task_id)
        task.alive_time = None
        db.session.commit()

    def start_task(self, task):
        if not self._lock_task(task):
            # can't lock, skip
            return

        thread = TaskThread(task)

        thread.start()
        self._active_tasks[task.id] = thread

    def stop_task(self, task_id: int):
        self._active_tasks[task_id].stop()
        del self._active_tasks[task_id]
        self._unlock_task(task_id)


def start(verbose=False):
    is_cloud = Config().get('cloud', False)
    if is_cloud is True:
        # disabled on cloud
        return

    monitor = TaskMonitor()
    monitor.start()


if __name__ == '__main__':
    start()

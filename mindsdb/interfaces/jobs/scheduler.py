import datetime as dt
import queue
import random
import threading
import time

from mindsdb.interfaces.jobs.jobs_controller import JobsExecutor
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.sentry import sentry_sdk  # noqa: F401

logger = log.getLogger(__name__)


def execute_async(q_in, q_out):

    while True:
        task = q_in.get()

        if task["type"] != "task":
            return

        record_id = task["record_id"]
        history_id = task["history_id"]

        executor = JobsExecutor()
        try:
            executor.execute_task_local(record_id, history_id)
        except (KeyboardInterrupt, SystemExit):
            q_out.put(True)
            raise

        except Exception:
            db.session.rollback()

        q_out.put(True)


class Scheduler:
    def __init__(self, config=None):
        self.config = config

        self.q_in = queue.Queue()
        self.q_out = queue.Queue()
        self.work_thread = threading.Thread(
            target=execute_async, args=(self.q_in, self.q_out), name='Scheduler.execute_async'
        )
        self.work_thread.start()

    def __del__(self):
        self.stop_thread()

    def stop_thread(self):
        self.q_in.put({"type": "exit"})

    def scheduler_monitor(self):
        check_interval = self.config.get("jobs", {}).get("check_interval", 30)

        while True:

            logger.debug("Scheduler check timetable")
            try:
                self.check_timetable()
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception as e:
                logger.error(e)

            # different instances should start in not the same time

            time.sleep(check_interval + random.randint(1, 10))

    def check_timetable(self):
        executor = JobsExecutor()

        exec_method = self.config.get("jobs", {}).get("executor", "local")

        for record in executor.get_next_tasks():
            logger.info(f"Job execute: {record.name}({record.id})")
            self.execute_task(record.id, exec_method)

        db.session.remove()

    def execute_task(self, record_id, exec_method):

        executor = JobsExecutor()
        if exec_method == "local":
            history_id = executor.lock_record(record_id)
            if history_id is None:
                # db.session.remove()
                logger.info(f"Unable create history record for {record_id}, is locked?")
                return

            # run in thread

            self.q_in.put(
                {
                    "type": "task",
                    "record_id": record_id,
                    "history_id": history_id,
                }
            )

            while True:
                try:
                    self.q_out.get(timeout=3)
                    break
                except queue.Empty:
                    # update last date:
                    history_record = db.JobsHistory.query.get(history_id)
                    history_record.updated_at = dt.datetime.now()
                    db.session.commit()

        else:
            # TODO add microservice mode
            raise NotImplementedError()

    def start(self):

        config = Config()
        db.init()
        self.config = config

        logger.info("Scheduler starts")

        try:
            self.scheduler_monitor()
        except (KeyboardInterrupt, SystemExit):

            self.stop_thread()
            pass


def start(verbose=False):
    logger.info("Jobs API is starting..")
    scheduler = Scheduler()

    scheduler.start()


if __name__ == "__main__":
    start()

import random
import time

import torch.multiprocessing as mp

from mindsdb.utilities.config import Config
from mindsdb.utilities.log import initialize_log
from mindsdb.utilities import log
from mindsdb.interfaces.storage import db

from mindsdb.interfaces.jobs.jobs_controller import JobsExecutor

mp_ctx = mp.get_context('spawn')

logger = log.get_log('jobs')


def scheduler_monitor(config):
    check_interval = config.get('jobs', {}).get('check_interval', 30)

    while True:

        logger.debug('Scheduler check timetable')
        try:
            check_timetable(config)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception as e:
            logger.error(e)

        # different instances should start in not the same time

        time.sleep(check_interval + random.randint(1, 10))


def check_timetable(config):
    scheduler = JobsExecutor()

    for record in scheduler.get_next_tasks():
        execute_sync(record, config)


def execute_sync(record, config):
    logger.info(f'Job execute: {record.name}({record.id})')

    exec_method = config.get('jobs', {}).get('executor', 'local')

    task_process(record.id, exec_method)


def execute_async(record, config):
    # if async is used with training in background then training is not happening
    #  maybe because 2 level of subprocess

    logger.info(f'Job execute: {record.name}({record.id})')

    exec_method = config.get('jobs', {}).get('executor', 'local')

    # run in subprocess
    p = mp_ctx.Process(
        target=task_process,
        args=(record.id, exec_method)
    )
    p.start()

    # for local and cloud we need to wait to prevent overload resources.
    p.join()


def task_process(record_id, exec_method):
    Config()
    db.init()
    # initialize_log(config, 'jobs', wrap_print=True)

    scheduler = JobsExecutor()
    if exec_method == 'local':
        try:
            history_id = scheduler.lock_record(record_id)
        except Exception as e:
            db.session.rollback()
            logger.error(f'Unable create history record, is locked? {e}')
            return

        scheduler.execute_task_local(record_id, history_id)

    else:
        # TODO add microservice mode
        raise NotImplementedError()


def start(verbose=False):
    config = Config()
    db.init()
    initialize_log(config, 'jobs', wrap_print=True)

    logger.info('Scheduler starts')
    scheduler_monitor(config)


if __name__ == '__main__':
    start()

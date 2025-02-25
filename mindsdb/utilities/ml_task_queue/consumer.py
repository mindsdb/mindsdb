import os
import time
import signal
import tempfile
import threading
from pathlib import Path
from functools import wraps
from collections.abc import Callable

import psutil
from walrus import Database
from pandas import DataFrame
from redis.exceptions import ConnectionError as RedisConnectionError

from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.integrations.libs.process_cache import process_cache
from mindsdb.utilities.ml_task_queue.utils import RedisKey, StatusNotifier, to_bytes, from_bytes
from mindsdb.utilities.ml_task_queue.base import BaseRedisQueue
from mindsdb.utilities.fs import clean_unlinked_process_marks
from mindsdb.utilities.functions import mark_process
from mindsdb.utilities.ml_task_queue.const import (
    ML_TASK_TYPE,
    ML_TASK_STATUS,
    TASKS_STREAM_NAME,
    TASKS_STREAM_CONSUMER_NAME,
    TASKS_STREAM_CONSUMER_GROUP_NAME
)
from mindsdb.utilities import log
from mindsdb.utilities.sentry import sentry_sdk  # noqa: F401

logger = log.getLogger(__name__)


def _save_thread_link(func: Callable) -> Callable:
    """ Decorator for MLTaskConsumer.
        Save thread in which func is executed to a list.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> None:
        current_thread = threading.current_thread()
        self._listen_message_threads.append(current_thread)
        try:
            result = func(self, *args, **kwargs)
        finally:
            self._listen_message_threads.remove(current_thread)
        return result
    return wrapper


class MLTaskConsumer(BaseRedisQueue):
    """ Listener of ML tasks queue and tasks executioner.
        Each new message waited and executed in separate thread.

        Attributes:
            _ready_event (Event): set if ready to start new queue listen thread
            _stop_event (Event): set if need to stop all threads/processes
            cpu_stat (list[float]): CPU usage statistic. Each value is 0-100 float representing CPU usage in %
            _collect_cpu_stat_thread (Thread): pointer to thread that collecting CPU usage statistic
            _listen_message_threads (list[Thread]): list of pointers to threads where queue messages are listening/processing
            db (Redis): database object
            cache: redis cache abstrtaction
            consumer_group: redis consumer group object
    """

    def __init__(self) -> None:
        self._ready_event = threading.Event()
        self._ready_event.set()

        self._stop_event = threading.Event()
        self._stop_event.clear()

        process_cache.init()

        # region collect cpu usage statistic
        self.cpu_stat = [0] * 10
        self._collect_cpu_stat_thread = threading.Thread(
            target=self._collect_cpu_stat, name='MLTaskConsumer._collect_cpu_stat'
        )
        self._collect_cpu_stat_thread.start()
        # endregion

        self._listen_message_threads = []

        # region connect to redis
        config = Config().get('ml_task_queue', {})
        self.db = Database(
            host=config.get('host', 'localhost'),
            port=config.get('port', 6379),
            db=config.get('db', 0),
            username=config.get('username'),
            password=config.get('password'),
            protocol=3
        )
        self.wait_redis_ping(60)

        self.db.Stream(TASKS_STREAM_NAME)
        self.cache = self.db.cache()
        self.consumer_group = self.db.consumer_group(TASKS_STREAM_CONSUMER_GROUP_NAME, [TASKS_STREAM_NAME])
        self.consumer_group.create()
        self.consumer_group.consumer(TASKS_STREAM_CONSUMER_NAME)
        # endregion

    def _collect_cpu_stat(self) -> None:
        """ Collect CPU usage statistic. Executerd in thread.
        """
        while self._stop_event.is_set() is False:
            self.cpu_stat = self.cpu_stat[1:]
            self.cpu_stat.append(psutil.cpu_percent())
            time.sleep(1)

    def get_avg_cpu_usage(self) -> float:
        """ get average CPU usage for last period (10s by default)

            Returns:
                float: 0-100 value, average CPU usage
        """
        return sum(self.cpu_stat) / len(self.cpu_stat)

    def wait_free_resources(self) -> None:
        """ Sleep in thread untill there are free resources. Checks:
            - avg CPU usage is less than 60%
            - current CPU usage is less than 60%
            - current tasks count is less than (N CPU cores) / 8
        """
        config = Config()
        is_cloud = config.get('cloud', False)
        processes_dir = Path(tempfile.gettempdir()).joinpath('mindsdb/processes/learn/')
        while True:
            while self.get_avg_cpu_usage() > 60 or max(self.cpu_stat[-3:]) > 60:
                time.sleep(1)
            if is_cloud and processes_dir.is_dir():
                clean_unlinked_process_marks()
                while (len(list(processes_dir.iterdir())) * 8) >= os.cpu_count():
                    time.sleep(1)
                    clean_unlinked_process_marks()
            if (self.get_avg_cpu_usage() > 60 or max(self.cpu_stat[-3:]) > 60) is False:
                return

    @_save_thread_link
    def _listen(self) -> None:
        """ Listen message queue untill get new message. Execute task.
        """
        message = None
        while message is None:
            self.wait_free_resources()
            self.wait_redis_ping()
            if self._stop_event.is_set():
                return

            try:
                message = self.consumer_group.read(count=1, block=1000, consumer=TASKS_STREAM_CONSUMER_NAME)
            except RedisConnectionError as e:
                logger.error(f"Can't connect to Redis: {e}")
                self._stop_event.set()
                return
            except Exception:
                self._stop_event.set()
                raise

            if message.get(TASKS_STREAM_NAME) is None or len(message.get(TASKS_STREAM_NAME)) == 0:
                message = None

        try:
            message = message[TASKS_STREAM_NAME][0][0]
            message_id = message[0].decode()
            message_content = message[1]
            self.consumer_group.streams[TASKS_STREAM_NAME].ack(message_id)
            self.consumer_group.streams[TASKS_STREAM_NAME].delete(message_id)

            payload = from_bytes(message_content[b'payload'])
            task_type = ML_TASK_TYPE(message_content[b'task_type'])
            model_id = int(message_content[b'model_id'])
            company_id = message_content[b'company_id']
            if len(company_id) == 0:
                company_id = None
            redis_key = RedisKey(message_content.get(b'redis_key'))

            # region read dataframe
            dataframe_bytes = self.cache.get(redis_key.dataframe)
            dataframe = None
            if dataframe_bytes is not None:
                dataframe = from_bytes(dataframe_bytes)
                self.cache.delete(redis_key.dataframe)
            # endregion

            ctx.load(payload['context'])
        finally:
            self._ready_event.set()

        try:
            task = process_cache.apply_async(
                task_type=task_type,
                model_id=model_id,
                payload=payload,
                dataframe=dataframe
            )
            status_notifier = StatusNotifier(redis_key, ML_TASK_STATUS.PROCESSING, self.db, self.cache)
            status_notifier.start()
            result = task.result()
        except Exception as e:
            self.wait_redis_ping()
            status_notifier.stop()
            exception_bytes = to_bytes(e)
            self.cache.set(redis_key.exception, exception_bytes, 10)
            self.db.publish(redis_key.status, ML_TASK_STATUS.ERROR.value)
            self.cache.set(redis_key.status, ML_TASK_STATUS.ERROR.value, 180)
        else:
            self.wait_redis_ping()
            status_notifier.stop()
            if isinstance(result, DataFrame):
                dataframe_bytes = to_bytes(result)
                self.cache.set(redis_key.dataframe, dataframe_bytes, 10)
            self.db.publish(redis_key.status, ML_TASK_STATUS.COMPLETE.value)
            self.cache.set(redis_key.status, ML_TASK_STATUS.COMPLETE.value, 180)

    def run(self) -> None:
        """ Start new listen thread each time when _ready_event is set
        """
        self._ready_event.set()
        while self._stop_event.is_set() is False:
            self._ready_event.wait(timeout=1)
            if self._ready_event.is_set() is False:
                continue
            self._ready_event.clear()
            threading.Thread(target=self._listen, name='MLTaskConsumer._listen').start()
        self.stop()

    def stop(self) -> None:
        """ Stop all executing threads
        """
        self._stop_event.set()
        for thread in (*self._listen_message_threads, self._collect_cpu_stat_thread):
            try:
                if thread.is_alive():
                    thread.join()
            except Exception:
                pass


@mark_process(name='internal', custom_mark='ml_task_consumer')
def start(verbose: bool) -> None:
    """ Create task queue consumer and start listen the queue
    """
    consumer = MLTaskConsumer()
    signal.signal(signal.SIGTERM, lambda _x, _y: consumer.stop())
    try:
        consumer.run()
    except Exception as e:
        consumer.stop()
        logger.error(f'Got exception: {e}', flush=True)
        raise
    finally:
        logger.info('Consumer process stopped', flush=True)

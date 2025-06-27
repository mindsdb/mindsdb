from collections.abc import Callable

import redis
from pandas import DataFrame

from mindsdb.utilities.ml_task_queue.utils import RedisKey, from_bytes
from mindsdb.utilities.ml_task_queue.const import ML_TASK_STATUS


class Task:
    """Abstraction for ML task. Should have interface similat to concurrent.futures.Future

    Attributes:
        db (Redis): database object
        redis_key (RedisKey): redis keys associated with task
        dataframe (DataFrame): task result
        exception (Exception): task exeuton  runtime exception
        _timeout (int): max time without status updating
    """

    def __init__(self, connection: redis.Redis, redis_key: RedisKey) -> None:
        self.db = connection
        self.redis_key = redis_key
        self.dataframe = None
        self.exception = None
        self._timeout = 60

    def subscribe(self) -> ML_TASK_STATUS:
        """return tasks status untill it is not done or failed"""
        pubsub = self.db.pubsub()
        cache = self.db.cache()
        pubsub.subscribe(self.redis_key.status)
        while msg := pubsub.get_message(timeout=self._timeout):
            if msg["type"] not in pubsub.PUBLISH_MESSAGE_TYPES:
                continue
            ml_task_status = ML_TASK_STATUS(msg["data"])
            if ml_task_status == ML_TASK_STATUS.COMPLETE:
                dataframe_bytes = cache.get(self.redis_key.dataframe)
                if dataframe_bytes is not None:
                    self.dataframe = from_bytes(dataframe_bytes)
                cache.delete(self.redis_key.dataframe)
            elif ml_task_status == ML_TASK_STATUS.ERROR:
                exception_bytes = cache.get(self.redis_key.exception)
                if exception_bytes is not None:
                    self.exception = from_bytes(exception_bytes)
            yield ml_task_status
        else:
            # there is no mesasges, timeout
            ml_task_status = ML_TASK_STATUS.TIMEOUT
            yield ml_task_status

    def wait(self, status: ML_TASK_STATUS = ML_TASK_STATUS.COMPLETE) -> None:
        """block threasd untill task is not done or failed"""
        for status in self.subscribe():
            if status in (ML_TASK_STATUS.WAITING, ML_TASK_STATUS.PROCESSING):
                continue
            if status == ML_TASK_STATUS.ERROR:
                if self.exception is not None:
                    raise self.exception
                else:
                    raise Exception("Unknown error during ML task execution")
            if status == ML_TASK_STATUS.TIMEOUT:
                raise Exception(f"Can't get answer in {self._timeout} seconds")
            if status == ML_TASK_STATUS.COMPLETE:
                return
            raise KeyError("Unknown task status")

    def result(self) -> DataFrame:
        """wait task is done and return result

        Returns:
            DataFrame: task result
        """
        self.wait()
        return self.dataframe

    def add_done_callback(self, fn: Callable) -> None:
        """need for compatability with concurrent.futures.Future interface"""
        pass

import time
import pickle
import threading

import psutil
from walrus import Database
from pandas import DataFrame

from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.integrations.libs.process_cache import process_cache
from mindsdb.utilities.ml_task_queue.utils import RedisKey, StatusNotifier, to_bytes, from_bytes
from mindsdb.utilities.ml_task_queue.const import (
    ML_TASK_TYPE,
    ML_TASK_STATUS,
    TASKS_STREAM_NAME,
    TASKS_STREAM_CONSUMER_NAME,
    TASKS_STREAM_CONSUMER_GROUP_NAME
)


class MLTaskConsumer:
    def __init__(self) -> None:
        # region preload ml handlers
        from mindsdb.interfaces.database.integrations import integration_controller
        config = Config()
        is_cloud = config.get('cloud', False)

        preload_hendlers = {}
        lightwood_handler = integration_controller.handler_modules['lightwood']
        if lightwood_handler.Handler is not None:
            preload_hendlers[lightwood_handler.Handler] = 4 if is_cloud else 1

        huggingface_handler = integration_controller.handler_modules['huggingface']
        if huggingface_handler.Handler is not None:
            preload_hendlers[huggingface_handler.Handler] = 1 if is_cloud else 0

        openai_handler = integration_controller.handler_modules['openai']
        if openai_handler.Handler is not None:
            preload_hendlers[openai_handler.Handler] = 1 if is_cloud else 0

        process_cache.init(preload_hendlers)
        # endregion

        # region collect cpu usage statistic
        self.cpu_stat = [0] * 10
        threading.Thread(target=self._collect_cpu_stat).start()
        # endregion

        # region connect to redis
        self.db = Database(protocol=3)
        try:
            self.db.ping()
        except ConnectionError:
            print('Cant connect to redis')
            raise
        self.db.Stream(TASKS_STREAM_NAME)
        self.cache = self.db.cache()
        self.consumer_group = self.db.consumer_group(TASKS_STREAM_CONSUMER_GROUP_NAME, [TASKS_STREAM_NAME])
        self.consumer_group.create()
        self.consumer_group.consumer(TASKS_STREAM_CONSUMER_NAME)
        # endregion

        self._ready_event = threading.Event()
        self._ready_event.set()

    def _collect_cpu_stat(self):
        self.cpu_stat = self.cpu_stat[1:]
        self.cpu_stat.append(psutil.cpu_percent())
        time.sleep(1)

    def get_avg_cpu_usage(self):
        """ get average CPU usage for last period (10s by default)

            Returns:
                float: 0-100 value, average CPU usage
        """
        return sum(self.cpu_stat) / len(self.cpu_stat)

    def wait_cpu_free(self):
        """ wait untill CPU usage will be low
        """
        while self.get_avg_cpu_usage() > 60 or max(self.cpu_stat[-3:]) > 60:
            time.sleep(1)

    def _listen(self):
        message = None
        while message is None:
            self.wait_cpu_free()
            message = self.consumer_group.read(count=1, block=1000, consumer=TASKS_STREAM_CONSUMER_NAME)
            if message.get(TASKS_STREAM_NAME) is None or len(message.get(TASKS_STREAM_NAME)) == 0:
                message = None

        try:
            message = message[TASKS_STREAM_NAME][0][0]
            message_id = message[0].decode()
            message_content = message[1]
            self.consumer_group.streams[TASKS_STREAM_NAME].ack(message_id)

            payload = pickle.loads(message_content[b'payload'])
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
            status_notifier.stop()
            exception_bytes = to_bytes(e)
            self.cache.set(redis_key.exception, exception_bytes, 10)
            self.db.publish(redis_key.status, ML_TASK_STATUS.ERROR.value)
            self.cache.set(redis_key.status, ML_TASK_STATUS.ERROR.value, 180)
        else:
            status_notifier.stop()
            if isinstance(result, DataFrame):
                dataframe_bytes = to_bytes(result)
                self.cache.set(redis_key.dataframe, dataframe_bytes, 10)
            self.db.publish(redis_key.status, ML_TASK_STATUS.COMPLETE.value)
            self.cache.set(redis_key.status, ML_TASK_STATUS.COMPLETE.value, 180)

    def run(self):
        self._ready_event.set()
        while True:
            self._ready_event.wait()
            self._ready_event.clear()
            threading.Thread(target=self._listen).start()


def start(verbose: bool):
    consumer = MLTaskConsumer()
    consumer.run()

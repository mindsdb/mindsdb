import io
import time
import pickle
import threading
from walrus import Database
from redis.exceptions import ConnectionError
import redis
from pandas import DataFrame
import socket

import psutil

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.process_cache import process_cache
from mindsdb.utilities.ml_task_queue.const import ML_TASK_TYPE, ML_TASK_STATUS


TASKS_STREAM_NAME = b'ml-tasks'
TASKS_STREAM_CONSUMER_GROUP_NAME = 'ml_executors'
TASKS_STREAM_CONSUMER_NAME = 'ml_executor'


def to_bytes(obj):
    return pickle.dumps(obj, protocol=5)


def from_bytes(b):
    return pickle.loads(b)


class RedisKey:
    @staticmethod
    def new():
        timestamp = str(time.time()).replace('.', '')
        return RedisKey(f"{timestamp}-{ctx.company_id}-{socket.gethostname()}".encode())

    def __init__(self, base_key: str):
        self._base_key = base_key

    @property
    def base(self):
        return self._base_key

    @property
    def status(self):
        return f'{self._base_key}-status'

    @property
    def dataframe(self):
        return f'{self._base_key}-dataframe'

    @property
    def exception(self):
        return f'{self._base_key}-exception'


class Task:
    def __init__(self, connection: redis.Redis, redis_key: RedisKey):
        self.db = connection
        self.redis_key = redis_key
        self.dataframe = None
        self.exception = None

    def subscribe(self):
        pubsub = self.db.pubsub()
        cache = self.db.cache()
        pubsub.subscribe(self.redis_key.status)
        while (msg := pubsub.get_message(timeout=30)):
            if msg['type'] not in pubsub.PUBLISH_MESSAGE_TYPES:
                continue
            ml_task_status = ML_TASK_STATUS(msg['data'])
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
            # there is no mesasges, timeout?
            ml_task_status = ML_TASK_STATUS.TIMEOUT
            yield ml_task_status

    def wait(self, status: ML_TASK_STATUS = ML_TASK_STATUS.COMPLETE):
        for status in self.subscribe():
            if status in (ML_TASK_STATUS.WAITING, ML_TASK_STATUS.PROCESSING):
                continue
            if status == ML_TASK_STATUS.ERROR:
                if self.exception is not None:
                    raise self.exception
                else:
                    raise Exception('Unknown error during ML task execution')
            if status == ML_TASK_STATUS.TIMEOUT:
                raise Exception()  # TODO
            if status == ML_TASK_STATUS.COMPLETE:
                return
            raise KeyError('Unknown task status')

    def result(self):
        self.wait()
        return self.dataframe

    def add_done_callback(self, fn: callable):
        # need for compatability with concurrent.futures.Future interface
        pass


class MLTaskProducer:
    def __init__(self) -> None:
        self.db = Database(protocol=3)  # decode_responses=True
        try:
            self.db.ping()
        except ConnectionError:
            print('Cant connect to redis')
            # raise
        self.stream = self.db.Stream(TASKS_STREAM_NAME)
        self.cache = self.db.cache()
        self.pubsub = self.db.pubsub()

    def apply_async(self, task_type: ML_TASK_TYPE, model_id: int, payload: dict, dataframe: DataFrame = None) -> object:
        '''
            Returns:
                str: task key in queue
        '''
        # only bytes, string, int or float. None is not supported
        try:
            # region payload to bytes
            f = io.BytesIO()
            pickle.dump(payload, f, protocol=5)
            f.seek(0)
            payload = f.read()
            # endregion

            redis_key = RedisKey.new()
            message = {
                "task_type": task_type.value,
                "company_id": '' if ctx.company_id is None else ctx.company_id,     # None can not be dumped
                "model_id": model_id,
                "payload": payload,
                "redis_key": redis_key.base
            }

            task = Task(self.db, redis_key)

            # data = {
            #     'status': ML_TASK_STATUS
            # }
            if dataframe is not None:
                dataframe_bytes = to_bytes(dataframe)
                # data['dataframe'] = dataframe_bytes
                self.cache.set(redis_key.dataframe, dataframe_bytes, 180)
            self.cache.set(redis_key.status, ML_TASK_STATUS.WAITING, 180)

            steam_message_id = self.stream.add(message)
            return task

        except ConnectionError:
            # TODO try to reconnect and send again?
            print('Cant send message to redis: connect failed')
            raise


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
        self.db = Database(protocol=3)  # decode_responses=True, 
        # db = redis.Redis(host='localhost', port=6379, db=0, protocol=3, decode_responses=True)
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
        # connect and process
        # TODO add here delay in case of overload TODO try/catch to whole
        message = None
        while message is None:
            self.wait_cpu_free()
            message = self.consumer_group.read(count=1, block=1000, consumer=TASKS_STREAM_CONSUMER_NAME)
            if message.get(TASKS_STREAM_NAME) is None or len(message.get(TASKS_STREAM_NAME)) == 0:
                message = None

        self.event.set()
        print('got message!')
        message = message[TASKS_STREAM_NAME][0][0]
        message_id = message[0].decode()
        message_content = message[1]
        # TODO xacn for msg

        # region deserialyze payload
        s = io.BytesIO(message_content[b'payload'])
        s.seek(0)
        payload = pickle.load(s)
        # endregion

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

        self.db.publish(redis_key.status, ML_TASK_STATUS.PROCESSING.value)
        self.cache.set(redis_key.status, ML_TASK_STATUS.PROCESSING.value, 180)
        task = process_cache.apply_async(
            task_type=task_type,
            model_id=model_id,
            payload=payload,
            dataframe=dataframe
        )
        try:
            result = task.result()
        except Exception as e:
            exception_bytes = to_bytes(e)
            self.cache.set(redis_key.exception, exception_bytes, 10)
            self.db.publish(redis_key.status, ML_TASK_STATUS.ERROR.value)
            self.cache.set(redis_key.status, ML_TASK_STATUS.ERROR.value, 180)
        else:
            if isinstance(result, DataFrame):
                dataframe_bytes = to_bytes(result)
                self.cache.set(redis_key.dataframe, dataframe_bytes, 10)
            self.db.publish(redis_key.status, ML_TASK_STATUS.COMPLETE.value)
            self.cache.set(redis_key.status, ML_TASK_STATUS.COMPLETE.value, 180)

    def run(self):
        self.event = threading.Event()
        self.event.set()
        while True:
            self.event.wait()  # timeout?
            self.event.clear()
            threading.Thread(target=self._listen).start()


ml_task_queue = MLTaskProducer()


def start(_x):
    consumer = MLTaskConsumer()
    consumer.run()

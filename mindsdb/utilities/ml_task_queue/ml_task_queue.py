import io
import pickle
import time
import importlib
import threading
from enum import Enum
from typing import Optional
from walrus import Database
from redis.exceptions import ConnectionError
import redis
from redis.client import PubSub
from pandas import DataFrame
import pyarrow as pa
import socket
from dataclasses import dataclass

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.learn_process import learn_process, predict_process
from mindsdb.integrations.handlers_client.ml_client_factory import MLClientFactory
from mindsdb.integrations.libs.process_cache import process_cache
from mindsdb.utilities.ml_task_queue.const import ML_TASK_TYPE, ML_TASK_STATUS


TASKS_STREAM_NAME = b'ml-tasks'
TASKS_STREAM_CONSUMER_GROUP_NAME = 'ml_executors'
TASKS_STREAM_CONSUMER_NAME = 'ml_executor'


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


class Task:
    def __init__(self, connection: redis.Redis, redis_key: RedisKey):
        self.db = connection
        self.redis_key = redis_key

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
                    self.dataframe = pa.deserialize(dataframe_bytes)
                cache.delete(self.redis_key.dataframe)
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
                raise Exception()  # TODO
            if status == ML_TASK_STATUS.TIMEOUT:
                raise Exception()  # TODO
            if status == ML_TASK_STATUS.COMPLETE:
                return
            raise KeyError('Unknown task status')

    def result(self):
        self.wait()
        # dataframe = 1
        return self.dataframe

    def add_done_callback(self, fn: callable):
        # need for compatability with concurrent.futures.Future interface
        pass


def task_in_thread(task_type, model_id, payload, dataframe, redis_db, redis_key):
    cache = redis_db.cache()
    redis_db.publish(redis_key.status, ML_TASK_STATUS.PROCESSING.value)
    cache.set(redis_key.status, ML_TASK_STATUS.PROCESSING.value, 180)
    task = process_cache.apply_async(
        task_type=task_type,
        model_id=model_id,
        payload=payload,
        dataframe=dataframe
    )
    result = task.result()
    if isinstance(result, DataFrame):
        dataframe_bytes = pa.serialize(result).to_buffer().to_pybytes()
        cache.set(redis_key.dataframe, dataframe_bytes, 10)
    redis_db.publish(redis_key.status, ML_TASK_STATUS.COMPLETE.value)
    cache.set(redis_key.status, ML_TASK_STATUS.COMPLETE.value, 180)


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
                dataframe_bytes = pa.serialize(dataframe).to_buffer().to_pybytes()
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

    def run(self):
        # connect
        db = Database(protocol=3)  # decode_responses=True, 
        # db = redis.Redis(host='localhost', port=6379, db=0, protocol=3, decode_responses=True)
        try:
            db.ping()
        except ConnectionError:
            print('Cant connect to redis')
            raise
        db.Stream(TASKS_STREAM_NAME)
        cache = db.cache()
        consumer_group = db.consumer_group(TASKS_STREAM_CONSUMER_GROUP_NAME, [TASKS_STREAM_NAME])
        consumer_group.create()
        consumer_group.consumer(TASKS_STREAM_CONSUMER_NAME)

        pubsub = db.pubsub()

        # x = self.stream.consumers_info(consumer_group)
        while True:
            # TODO add here delay in case of overload TODO try/catch to whole
            message = consumer_group.read(count=1, block=1000, consumer=TASKS_STREAM_CONSUMER_NAME)
            if message.get(TASKS_STREAM_NAME) is None or len(message.get(TASKS_STREAM_NAME)) == 0:
                continue
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
            dataframe_bytes = cache.get(redis_key.dataframe)
            dataframe = None
            if dataframe_bytes is not None:
                dataframe = pa.deserialize(dataframe_bytes)
                cache.delete(redis_key.dataframe)
            # endregion

            context = payload['context']  # TODO

            # +++
            # task = process_cache.apply_async(
            #     task_type=task_type,
            #     model_id=model_id,
            #     payload=payload,
            #     dataframe=dataframe
            # )
            try:
                thread = threading.Thread(target=task_in_thread, kwargs={
                    'task_type': task_type,
                    'model_id': model_id,
                    'payload': payload,
                    'dataframe': dataframe,
                    'redis_db': db,
                    'redis_key': redis_key
                })
            except Exception as e:
                x = 1
            thread.start()
            continue
            # ---


ml_task_queue = MLTaskProducer()


def start(_x):
    consumer = MLTaskConsumer()
    consumer.run()

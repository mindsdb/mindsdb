import time
import pickle
import socket
import threading

from mindsdb.utilities.context import context as ctx


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


class StatusNotifier(threading.Thread):
    def __init__(self, redis_key, ml_task_status, db, cache):
        threading.Thread.__init__(self)
        self.redis_key = redis_key
        self.ml_task_status = ml_task_status
        self.db = db
        self.cache = cache
        self._stop_event = threading.Event()

    def set_status(self, ml_task_status):
        self.ml_task_status = ml_task_status

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.is_set():
            self.db.publish(self.redis_key.status, self.ml_task_status.value)
            self.cache.set(self.redis_key.status, self.ml_task_status.value, 180)
            time.sleep(5)

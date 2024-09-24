import time
import pickle
import socket
import threading

from walrus import Database
from redis.exceptions import ConnectionError as RedisConnectionError

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.ml_task_queue.const import ML_TASK_STATUS
from mindsdb.utilities.sentry import sentry_sdk  # noqa: F401


def to_bytes(obj: object) -> bytes:
    """ dump object into bytes

        Args:
            obj (object): object to convert

        Returns:
            bytes
    """
    return pickle.dumps(obj, protocol=5)


def from_bytes(b: bytes) -> object:
    """ load object from bytes

        Args:
            b (bytes):

        Returns:
            object
    """
    return pickle.loads(b)


def wait_redis_ping(db: Database, timeout: int = 30):
    """ Wait when redis.ping return True

        Args:
            db (Database): redis db object
            timeout (int): seconds to wait for success ping

        Raises:
            RedisConnectionError: if `ping` did not return `True` within `timeout` seconds
    """
    end_time = time.time() + timeout
    while time.time() <= end_time:
        try:
            if db.ping() is True:
                break
        except RedisConnectionError:
            pass
        time.sleep(2)
    else:
        raise RedisConnectionError


class RedisKey:
    """ The class responsible for unique task keys in redis

        Attributes:
            _base_key (bytes): prefix for keys
    """

    @staticmethod
    def new() -> 'RedisKey':
        timestamp = str(time.time()).replace('.', '')
        return RedisKey(f"{timestamp}-{ctx.company_id}-{socket.gethostname()}".encode())

    def __init__(self, base_key: bytes) -> None:
        self._base_key = base_key

    @property
    def base(self) -> bytes:
        return self._base_key

    @property
    def status(self) -> str:
        return (self._base_key + b'-status').decode()

    @property
    def dataframe(self) -> str:
        return (self._base_key + b'-dataframe').decode()

    @property
    def exception(self) -> str:
        return (self._base_key + b'-exception').decode()


class StatusNotifier(threading.Thread):
    """ Worker that updates task status in redis with fixed frequency
    """

    def __init__(self, redis_key: RedisKey, ml_task_status: ML_TASK_STATUS, db, cache) -> None:
        threading.Thread.__init__(self)
        self.redis_key = redis_key
        self.ml_task_status = ml_task_status
        self.db = db
        self.cache = cache
        self._stop_event = threading.Event()

    def set_status(self, ml_task_status: ML_TASK_STATUS):
        """ change status

            Args:
                ml_task_status (ML_TASK_STATUS): new status
        """
        self.ml_task_status = ml_task_status

    def stop(self) -> None:
        """ stop status updating
        """
        self._stop_event.set()

    def run(self):
        """ start update status with fixed frequency
        """
        while not self._stop_event.is_set():
            wait_redis_ping(self.db)
            self.db.publish(self.redis_key.status, self.ml_task_status.value)
            self.cache.set(self.redis_key.status, self.ml_task_status.value, 180)
            time.sleep(5)

from mindsdb.utilities.ml_task_queue.utils import wait_redis_ping


class BaseRedisQueue:
    def wait_redis_ping(self, timeout: int = 30) -> None:
        """ wait when redis.ping return True

            Args:
                timeout (int): seconds to wait for success ping

            Raises:
                RedisConnectionError: if `ping` did not return `True` within `timeout` seconds
        """
        return wait_redis_ping(self.db, timeout)

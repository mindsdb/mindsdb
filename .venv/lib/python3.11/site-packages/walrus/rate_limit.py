import hashlib
import pickle
import time
from functools import wraps


class RateLimitException(Exception):
    pass


class RateLimit(object):
    """
    Rate limit implementation. Allows up to "limit" number of events every per
    the given number of seconds.
    """
    def __init__(self, database, name, limit=5, per=60, debug=False):
        """
        :param database: :py:class:`Database` instance.
        :param name: Namespace for this cache.
        :param int limit: Number of events allowed during a given time period.
        :param int per: Time period the ``limit`` applies to, in seconds.
        :param debug: Disable rate-limit for debugging purposes. All events
                      will appear to be allowed and valid.
        """
        self.database = database
        self.name = name
        self._limit = limit
        self._per = per
        self._debug = debug

    def limit(self, key):
        """
        Function to log an event with the given key. If the ``key`` has not
        exceeded their allotted events, then the function returns ``False`` to
        indicate that no limit is being imposed.

        If the ``key`` has exceeded the number of events, then the function
        returns ``True`` indicating rate-limiting should occur.

        :param str key: A key identifying the source of the event.
        :returns: Boolean indicating whether the event should be rate-limited
            or not.
        """
        if self._debug:
            return False

        counter = self.database.List(self.name + ':' + key)
        n = len(counter)
        is_limited = False
        if n < self._limit:
            counter.prepend(str(time.time()))
        else:
            oldest = counter[-1]
            if (oldest is not None) and (time.time() - float(oldest) < self._per):
                is_limited = True
            else:
                counter.prepend(str(time.time()))
            del counter[:self._limit]
        counter.pexpire(int(self._per * 2000))
        return is_limited

    def rate_limited(self, key_function=None):
        """
        Function or method decorator that will prevent calls to the decorated
        function when the number of events has been exceeded for the given
        time period.

        It is probably important that you take care to choose an appropriate
        key function. For instance, if rate-limiting a web-page you might use
        the requesting user's IP as the key.

        If the number of allowed events has been exceeded, a
        ``RateLimitException`` will be raised.

        :param key_function: Function that accepts the params of the decorated
            function and returns a string key. If not provided, a hash of the
            args and kwargs will be used.
        :returns: If the call is not rate-limited, then the return value will
            be that of the decorated function.
        :raises: ``RateLimitException``.
        """
        if key_function is None:
            def key_function(*args, **kwargs):
                data = pickle.dumps((args, sorted(kwargs.items())))
                return hashlib.md5(data).hexdigest()

        def decorator(fn):
            @wraps(fn)
            def inner(*args, **kwargs):
                key = key_function(*args, **kwargs)
                if self.limit(key):
                    raise RateLimitException(
                        'Call to %s exceeded %s events in %s seconds.' % (
                            fn.__name__, self._limit, self._per))
                return fn(*args, **kwargs)
            return inner
        return decorator


class RateLimitLua(RateLimit):
    """
    Rate limit implementation. Allows up to "limit" number of events every per
    the given number of seconds. Uses a Lua script to ensure atomicity.
    """
    def limit(self, key):
        if self._debug:
            return False

        key = self.name + ':' + key
        return bool(self.database.run_script(
            'rate_limit',
            keys=[key],
            args=[self._limit, self._per, time.time()]))

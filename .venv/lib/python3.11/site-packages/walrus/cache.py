from functools import wraps
import hashlib
import pickle
import sys
import threading
import time
try:
    from Queue import Queue  # Python 2
except ImportError:
    from queue import Queue  # Python 3

from walrus.utils import decode
from walrus.utils import encode
from walrus.utils import PY3

if PY3:
    imap = map
else:
    from itertools import imap



class Cache(object):
    """
    Cache implementation with simple ``get``/``set`` operations,
    and a decorator.
    """
    def __init__(self, database, name='cache', default_timeout=None,
                 debug=False):
        """
        :param database: :py:class:`Database` instance.
        :param name: Namespace for this cache.
        :param int default_timeout: Default cache timeout.
        :param debug: Disable cache for debugging purposes. Cache will no-op.
        """
        self.database = database
        self.name = name
        self.prefix_len = len(self.name) + 1
        self.default_timeout = default_timeout
        self.debug = debug
        self.metrics = {'hits': 0, 'misses': 0, 'writes': 0}

    def make_key(self, s):
        return ':'.join((self.name, s))

    def unmake_key(self, k):
        return k[self.prefix_len:]

    def get(self, key, default=None):
        """
        Retreive a value from the cache. In the event the value
        does not exist, return the ``default``.
        """
        key = self.make_key(key)

        if self.debug:
            return default

        try:
            value = self.database[key]
        except KeyError:
            self.metrics['misses'] += 1
            return default
        else:
            self.metrics['hits'] += 1
            return pickle.loads(value)

    def set(self, key, value, timeout=None):
        """
        Cache the given ``value`` in the specified ``key``. If no
        timeout is specified, the default timeout will be used.
        """
        key = self.make_key(key)
        if timeout is None:
            timeout = self.default_timeout

        if self.debug:
            return True

        pickled_value = pickle.dumps(value)
        self.metrics['writes'] += 1
        if timeout:
            return self.database.setex(key, int(timeout), pickled_value)
        else:
            return self.database.set(key, pickled_value)

    def delete(self, key):
        """Remove the given key from the cache."""
        if self.debug: return 0
        return self.database.delete(self.make_key(key))

    def get_many(self, keys):
        """
        Retrieve multiple values from the cache. Missing keys are not included
        in the result dictionary.

        :param list keys: list of keys to fetch.
        :returns: dictionary mapping keys to cached values.
        """
        accum = {}
        if self.debug: return accum

        prefixed = [self.make_key(key) for key in keys]
        for key, value in zip(keys, self.database.mget(prefixed)):
            if value is not None:
                accum[key] = pickle.loads(value)
                self.metrics['hits'] += 1
            else:
                self.metrics['misses'] += 1
        return accum

    def set_many(self, __data=None, timeout=None, **kwargs):
        """
        Set multiple key/value pairs in one operation.

        :param dict __data: provide data as dictionary of key/value pairs.
        :param timeout: optional timeout for data.
        :param kwargs: alternatively, provide data as keyword arguments.
        :returns: True on success.
        """
        if self.debug:
            return True

        timeout = timeout if timeout is not None else self.default_timeout
        if __data is not None:
            kwargs.update(__data)

        accum = {}
        for key, value in kwargs.items():
            accum[self.make_key(key)] = pickle.dumps(value)

        pipeline = self.database.pipeline()
        pipeline.mset(accum)
        if timeout:
            for key in accum:
                pipeline.expire(key, timeout)

        self.metrics['writes'] += len(accum)
        return pipeline.execute()[0]

    def delete_many(self, keys):
        """
        Delete multiple keys from the cache in one operation.

        :param list keys: keys to delete.
        :returns: number of keys removed.
        """
        if self.debug: return
        prefixed = [self.make_key(key) for key in keys]
        return self.database.delete(*prefixed)

    def keys(self):
        """
        Return all keys for cached values.
        """
        return imap(decode, self.database.keys(self.make_key('') + '*'))

    def flush(self):
        """Remove all cached objects from the database."""
        keys = list(self.keys())
        if keys:
            return self.database.delete(*keys)

    def incr(self, key, delta=1):
        return self.database.incr(self.make_key(key), delta)

    def _key_fn(a, k):
        return hashlib.md5(pickle.dumps((a, k))).hexdigest()

    def cached(self, key_fn=_key_fn, timeout=None, metrics=False):
        """
        Decorator that will transparently cache calls to the
        wrapped function. By default, the cache key will be made
        up of the arguments passed in (like memoize), but you can
        override this by specifying a custom ``key_fn``.

        :param key_fn: Function used to generate a key from the
            given args and kwargs.
        :param timeout: Time to cache return values.
        :param metrics: Keep stats on cache utilization and timing.
        :returns: Return the result of the decorated function
            call with the given args and kwargs.

        Usage::

            cache = Cache(my_database)

            @cache.cached(timeout=60)
            def add_numbers(a, b):
                return a + b

            print add_numbers(3, 4)  # Function is called.
            print add_numbers(3, 4)  # Not called, value is cached.

            add_numbers.bust(3, 4)  # Clear cache for (3, 4).
            print add_numbers(3, 4)  # Function is called.

        The decorated function also gains a new attribute named
        ``bust`` which will clear the cache for the given args.
        """
        def decorator(fn):
            def make_key(args, kwargs):
                return '%s:%s' % (fn.__name__, key_fn(args, kwargs))

            def bust(*args, **kwargs):
                return self.delete(make_key(args, kwargs))

            _metrics = {
                'hits': 0,
                'misses': 0,
                'avg_hit_time': 0,
                'avg_miss_time': 0}

            @wraps(fn)
            def inner(*args, **kwargs):
                start = time.time()
                is_cache_hit = True
                key = make_key(args, kwargs)
                res = self.get(key, sentinel)
                if res is sentinel:
                    res = fn(*args, **kwargs)
                    self.set(key, res, timeout)
                    is_cache_hit = False

                if metrics:
                    dur = time.time() - start
                    if is_cache_hit:
                        _metrics['hits'] += 1
                        _metrics['avg_hit_time'] += (dur / _metrics['hits'])
                    else:
                        _metrics['misses'] += 1
                        _metrics['avg_miss_time'] += (dur / _metrics['misses'])

                return res

            inner.bust = bust
            inner.make_key = make_key
            if metrics:
                inner.metrics = _metrics
            return inner
        return decorator

    def cached_property(self, key_fn=_key_fn, timeout=None):
        """
        Decorator that will transparently cache calls to the wrapped
        method. The method will be exposed as a property.

        Usage::

            cache = Cache(my_database)

            class Clock(object):
                @cache.cached_property()
                def now(self):
                    return datetime.datetime.now()

            clock = Clock()
            print clock.now
        """
        this = self

        class _cached_property(object):
            def __init__(self, fn):
                self._fn = this.cached(key_fn, timeout)(fn)

            def __get__(self, instance, instance_type=None):
                if instance is None:
                    return self
                return self._fn(instance)

            def __delete__(self, obj):
                self._fn.bust(obj)

            def __set__(self, instance, value):
                raise ValueError('Cannot set value of a cached property.')

        def decorator(fn):
            return _cached_property(fn)

        return decorator

    def cache_async(self, key_fn=_key_fn, timeout=3600):
        """
        Decorator that will execute the cached function in a separate
        thread. The function will immediately return, returning a
        callable to the user. This callable can be used to check for
        a return value.

        For details, see the :ref:`cache-async` section of the docs.

        :param key_fn: Function used to generate cache key.
        :param int timeout: Cache timeout in seconds.
        :returns: A new function which can be called to retrieve the
            return value of the decorated function.
        """
        def decorator(fn):
            wrapped = self.cached(key_fn, timeout)(fn)

            @wraps(fn)
            def inner(*args, **kwargs):
                q = Queue()
                def _sub_fn():
                    q.put(wrapped(*args, **kwargs))
                def _get_value(block=True, timeout=None):
                    if not hasattr(_get_value, '_return_value'):
                        result = q.get(block=block, timeout=timeout)
                        _get_value._return_value = result
                    return _get_value._return_value

                thread = threading.Thread(target=_sub_fn)
                thread.start()
                return _get_value
            return inner
        return decorator


class sentinel(object):
    pass

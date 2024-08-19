from functools import wraps
import glob
import os
import sys
import threading
import uuid

try:
    from redis import Redis
    from redis.exceptions import ConnectionError
    from redis.exceptions import TimeoutError
except ImportError:
    Redis = object
    ConnectionError = TimeoutError = Exception

from walrus.autocomplete import Autocomplete
from walrus.cache import Cache
from walrus.containers import Array
from walrus.containers import BitField
from walrus.containers import BloomFilter
from walrus.containers import ConsumerGroup
from walrus.containers import Hash
from walrus.containers import HyperLogLog
from walrus.containers import List
from walrus.containers import Set
from walrus.containers import Stream
from walrus.containers import ZSet
from walrus.counter import Counter
from walrus.fts import Index
from walrus.graph import Graph
from walrus.lock import Lock
from walrus.rate_limit import RateLimit
from walrus.rate_limit import RateLimitLua
from walrus.streams import TimeSeries


class TransactionLocal(threading.local):
    def __init__(self, **kwargs):
        super(TransactionLocal, self).__init__(**kwargs)
        self.pipes = []

    @property
    def pipe(self):
        if len(self.pipes):
            return self.pipes[-1]

    def commit(self):
        pipe = self.pipes.pop()
        return pipe.execute()

    def abort(self):
        pipe = self.pipes.pop()
        pipe.reset()


class Database(Redis):
    """
    Redis-py client with some extras.
    """
    def __init__(self, *args, **kwargs):
        """
        :param args: Arbitrary positional arguments to pass to the
            base ``Redis`` instance.
        :param kwargs: Arbitrary keyword arguments to pass to the
            base ``Redis`` instance.
        :param str script_dir: Path to directory containing walrus
            scripts. Use "script_dir=False" to disable loading any scripts.
        """
        script_dir = kwargs.pop('script_dir', None)
        super(Database, self).__init__(*args, **kwargs)
        self.__mapping = {
            b'list': self.List,
            b'set': self.Set,
            b'zset': self.ZSet,
            b'hash': self.Hash}
        self._transaction_local = TransactionLocal()
        self._transaction_lock = threading.RLock()
        if script_dir is not False:
            self.init_scripts(script_dir=script_dir)

    def __bool__(self):
        return True  # Avoid falling back to __len__().

    def xsetid(self, name, id):
        """
        Set the last ID of the given stream.

        :param name: stream identifier
        :param id: new value for last ID
        """
        return self.execute_command('XSETID', name, id) == b'OK'

    def xpending_summary(self, key, group):
        """
        Pending message summary report.

        :param key: stream identifier
        :param group: consumer group name
        :returns: dictionary of information about pending messages
        """
        return self.xpending(key, group)

    def get_transaction(self):
        with self._transaction_lock:
            local = self._transaction_local
            local.pipes.append(self.pipeline())
            return local.pipe

    def commit_transaction(self):
        """
        Commit the currently active transaction (Pipeline). If no
        transaction is active in the current thread, an exception
        will be raised.

        :returns: The return value of executing the Pipeline.
        :raises: ``ValueError`` if no transaction is active.
        """
        with self._transaction_lock:
            local = self._transaction_local
            if not local.pipes:
                raise ValueError('No transaction is currently active.')
            return local.commit()

    def clear_transaction(self):
        """
        Clear the currently active transaction (if exists). If the
        transaction stack is not empty, then a new pipeline will
        be initialized.

        :returns: No return value.
        :raises: ``ValueError`` if no transaction is active.
        """
        with self._transaction_lock:
            local = self._transaction_local
            if not local.pipes:
                raise ValueError('No transaction is currently active.')
            local.abort()

    def atomic(self):
        return _Atomic(self)

    def init_scripts(self, script_dir=None):
        self._scripts = {}
        if not script_dir:
            script_dir = os.path.join(os.path.dirname(__file__), 'scripts')
        for filename in glob.glob(os.path.join(script_dir, '*.lua')):
            with open(filename, 'r') as fh:
                script_obj = self.register_script(fh.read())
                script_name = os.path.splitext(os.path.basename(filename))[0]
                self._scripts[script_name] = script_obj

    def run_script(self, script_name, keys=None, args=None):
        """
        Execute a walrus script with the given arguments.

        :param script_name: The base name of the script to execute.
        :param list keys: Keys referenced by the script.
        :param list args: Arguments passed in to the script.
        :returns: Return value of script.

        .. note:: Redis scripts require two parameters, ``keys``
            and ``args``, which are referenced in lua as ``KEYS``
            and ``ARGV``.
        """
        return self._scripts[script_name](keys, args)

    def get_temp_key(self):
        """
        Generate a temporary random key using UUID4.
        """
        return 'temp.%s' % uuid.uuid4()

    def __iter__(self):
        """
        Iterate over the keys of the selected database.
        """
        return iter(self.scan_iter())

    def __len__(self):
        return self.dbsize()

    def search(self, pattern):
        """
        Search the keyspace of the selected database using the
        given search pattern.

        :param str pattern: Search pattern using wildcards.
        :returns: Iterator that yields matching keys.
        """
        return self.scan_iter(pattern)

    def get_key(self, key):
        """
        Return a rich object for the given key. For instance, if
        a hash key is requested, then a :py:class:`Hash` will be
        returned.

        Note: only works for Hash, List, Set and ZSet.

        :param str key: Key to retrieve.
        :returns: A hash, set, list, zset or array.
        """
        return self.__mapping.get(self.type(key), self.__getitem__)(key)

    def hash_exists(self, key):
        return self.exists(key)

    def autocomplete(self, namespace='autocomplete', **kwargs):
        return Autocomplete(self, namespace, **kwargs)

    def cache(self, name='cache', default_timeout=3600):
        """
        Create a :py:class:`Cache` instance.

        :param str name: The name used to prefix keys used to
            store cached data.
        :param int default_timeout: The default key expiry.
        :returns: A :py:class:`Cache` instance.
        """
        return Cache(self, name=name, default_timeout=default_timeout)

    def counter(self, name):
        """
        Create a :py:class:`Counter` instance.

        :param str name: The name used to store the counter's value.
        :returns: A :py:class:`Counter` instance.
        """
        return Counter(self, name=name)

    def graph(self, name, *args, **kwargs):
        """
        Creates a :py:class:`Graph` instance.

        :param str name: The namespace for the graph metadata.
        :returns: a :py:class:`Graph` instance.
        """
        return Graph(self, name, *args, **kwargs)

    def lock(self, name, ttl=None, lock_id=None):
        """
        Create a named :py:class:`Lock` instance. The lock implements
        an API similar to the standard library's ``threading.Lock``,
        and can also be used as a context manager or decorator.

        :param str name: The name of the lock.
        :param int ttl: The time-to-live for the lock in milliseconds
            (optional). If the ttl is ``None`` then the lock will not
            expire.
        :param str lock_id: Optional identifier for the lock instance.
        """
        return Lock(self, name, ttl, lock_id)

    def rate_limit(self, name, limit=5, per=60, debug=False):
        """
        Rate limit implementation. Allows up to `limit` of events every `per`
        seconds.

        See :ref:`rate-limit` for more information.
        """
        return RateLimit(self, name, limit, per, debug)

    def rate_limit_lua(self, name, limit=5, per=60, debug=False):
        """
        Rate limit implementation. Allows up to `limit` of events every `per`
        seconds. Uses a Lua script for atomicity.

        See :ref:`rate-limit` for more information.
        """
        return RateLimitLua(self, name, limit, per, debug)

    def Index(self, name, **options):
        """
        Create a :py:class:`Index` full-text search index with the given
        name and options.
        """
        return Index(self, name, **options)

    def List(self, key):
        """
        Create a :py:class:`List` instance wrapping the given key.
        """
        return List(self, key)

    def Hash(self, key):
        """
        Create a :py:class:`Hash` instance wrapping the given key.
        """
        return Hash(self, key)

    def Set(self, key):
        """
        Create a :py:class:`Set` instance wrapping the given key.
        """
        return Set(self, key)

    def ZSet(self, key):
        """
        Create a :py:class:`ZSet` instance wrapping the given key.
        """
        return ZSet(self, key)

    def HyperLogLog(self, key):
        """
        Create a :py:class:`HyperLogLog` instance wrapping the given
        key.
        """
        return HyperLogLog(self, key)

    def Array(self, key):
        """
        Create a :py:class:`Array` instance wrapping the given key.
        """
        return Array(self, key)

    def Stream(self, key):
        """
        Create a :py:class:`Stream` instance wrapping the given key.
        """
        return Stream(self, key)

    def consumer_group(self, group, keys, consumer=None):
        """
        Create a named :py:class:`ConsumerGroup` instance for the given key(s).

        :param group: name of consumer group
        :param keys: stream identifier(s) to monitor. May be a single stream
            key, a list of stream keys, or a key-to-minimum id mapping. The
            minimum id for each stream should be considered an exclusive
            lower-bound. The '$' value can also be used to only read values
            added *after* our command started blocking.
        :param consumer: name for consumer within group
        :returns: a :py:class:`ConsumerGroup` instance
        """
        return ConsumerGroup(self, group, keys, consumer=consumer)

    def time_series(self, group, keys, consumer=None):
        """
        Create a named :py:class:`TimeSeries` consumer-group for the
        given key(s). TimeSeries objects are almost identical to
        :py:class:`ConsumerGroup` except they offer a higher level of
        abstraction and read/write message ids as datetimes.

        :param group: name of consumer group
        :param keys: stream identifier(s) to monitor. May be a single stream
            key, a list of stream keys, or a key-to-minimum id mapping. The
            minimum id for each stream should be considered an exclusive
            lower-bound. The '$' value can also be used to only read values
            added *after* our command started blocking.
        :param consumer: name for consumer within group
        :returns: a :py:class:`TimeSeries` instance
        """
        return TimeSeries(self, group, keys, consumer=consumer)

    def bit_field(self, key):
        """
        Container for working with the Redis BITFIELD command.

        :returns: a :py:class:`BitField` instance.
        """
        return BitField(self, key)

    def bloom_filter(self, key, size=64 * 1024):
        """
        Create a :py:class:`BloomFilter` container type.

        Bloom-filters are probabilistic data-structures that are used to answer
        the question: "is X a member of set S?" It is possible to receive a
        false positive, but impossible to receive a false negative (in other
        words, if the bloom filter contains a value, it will never erroneously
        report that it does *not* contain such a value). The accuracy of the
        bloom-filter and the likelihood of a false positive can be reduced by
        increasing the size of the bloomfilter. The default size is 64KB (or
        524,288 bits).
        """
        return BloomFilter(self, key, size)

    def cas(self, key, value, new_value):
        """
        Perform an atomic compare-and-set on the value in "key", using a prefix
        match on the provided value.
        """
        return self.run_script('cas', keys=[key], args=[value, new_value])

    def listener(self, channels=None, patterns=None, is_async=False):
        """
        Decorator for wrapping functions used to listen for Redis
        pub-sub messages.

        The listener will listen until the decorated function
        raises a ``StopIteration`` exception.

        :param list channels: Channels to listen on.
        :param list patterns: Patterns to match.
        :param bool is_async: Whether to start the listener in a
            separate thread.
        """
        def decorator(fn):
            _channels = channels or []
            _patterns = patterns or []

            @wraps(fn)
            def inner():
                pubsub = self.pubsub()

                def listen():
                    for channel in _channels:
                        pubsub.subscribe(channel)
                    for pattern in _patterns:
                        pubsub.psubscribe(pattern)

                    for data_dict in pubsub.listen():
                        try:
                            ret = fn(**data_dict)
                        except StopIteration:
                            pubsub.close()
                            break

                if is_async:
                    worker = threading.Thread(target=listen)
                    worker.start()
                    return worker
                else:
                    listen()

            return inner
        return decorator

    def stream_log(self, callback, connection_id='monitor'):
        """
        Stream Redis activity one line at a time to the given
        callback.

        :param callback: A function that accepts a single argument,
            the Redis command.
        """
        conn = self.connection_pool.get_connection(connection_id, None)
        conn.send_command('monitor')
        while callback(conn.read_response()):
            pass


class _Atomic(object):
    def __init__(self, db):
        self.db = db

    @property
    def pipe(self):
        return self.db._transaction_local.pipe

    def __enter__(self):
        self.db.get_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.clear(False)
        else:
            self.commit(False)

    def commit(self, begin_new=True):
        ret = self.db.commit_transaction()
        if begin_new:
            self.db.get_transaction()
        return ret

    def clear(self, begin_new=True):
        self.db.clear_transaction()
        if begin_new:
            self.db.get_transaction()

from functools import wraps
import os


class Lock(object):
    """
    Lock implementation. Can also be used as a context-manager or
    decorator.

    Unlike the redis-py lock implementation, this Lock does not
    use a spin-loop when blocking to acquire the lock. Instead,
    it performs a blocking pop on a list. When a lock is released,
    a value is pushed into this list, signalling that the lock is
    available.

    .. warning::
        The event list for each lock persists
        indefinitely unless removed using :py:meth:`Lock.clear` or
        otherwise manually in the Redis database. For this reason,
        be cautious when creating locks dynamically, or your
        keyspace might grow in an unbounded way.

    The lock uses Lua scripts to ensure the atomicity of its
    operations.

    You can set a TTL on a lock to reduce the potential for deadlocks
    in the event of a crash. If a lock is not released before it
    exceeds its TTL, and threads that are blocked waiting for the
    lock could potentially re-acquire it.

    .. note:: TTL is specified in **milliseconds**.

    Locks can be used as context managers or as decorators:

    .. code-block:: python

        lock = db.lock('my-lock')

        with lock:
            perform_some_calculations()

        @lock
        def another_function():
            # The lock will be acquired when this function is
            # called, and released when the function returns.
            do_some_more_calculations()
    """
    def __init__(self, database, name, ttl=None, lock_id=None):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the lock.
        :param int ttl: The time-to-live for the lock in milliseconds.
        :param str lock_id: Unique identifier for the lock instance.
        """
        self.database = database
        self.name = name
        self.ttl = ttl or 0
        self._lock_id = lock_id or os.urandom(32)

    @property
    def key(self):
        return 'lock:%s' % (self.name)

    @property
    def event(self):
        return 'lock.event:%s' % (self.name)

    def acquire(self, block=True):
        """
        Acquire the lock. The lock will be held until it is released
        by calling :py:meth:`Lock.release`. If the lock was
        initialized with a ``ttl``, then the lock will be released
        automatically after the given number of milliseconds.

        By default this method will block until the lock becomes
        free (either by being released or expiring). The blocking is
        accomplished by performing a blocking left-pop on a list, as
        opposed to a spin-loop.

        If you specify ``block=False``, then the method will return
        ``False`` if the lock could not be acquired.

        :param bool block: Whether to block while waiting to acquire
            the lock.
        :returns: Returns ``True`` if the lock was acquired.
        """
        event_wait = self.ttl // 1000 if self.ttl else 1

        while True:
            acquired = self.database.run_script(
                'lock_acquire',
                keys=[self.key],
                args=[self._lock_id, self.ttl])
            if acquired == 1 or not block:
                return acquired == 1

            # Perform a blocking pop on the event key. When a lock
            # is released, a value is pushed into the list, which
            # signals listeners that the lock is available.
            self.database.blpop(self.event, event_wait)

    def release(self):
        """
        Release the lock.

        :returns: Returns ``True`` if the lock was released.
        """
        unlocked = self.database.run_script(
            'lock_release',
            keys=[self.key, self.event],
            args=[self._lock_id])
        return unlocked != 0

    def clear(self):
        """
        Clear the lock, allowing it to be acquired. Do not use this
        method except to recover from a deadlock. Otherwise you should
        use :py:meth:`Lock.release`.
        """
        self.database.delete(self.key)
        self.database.delete(self.event)

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.release():
            raise RuntimeError('Error releasing lock "%s".' % self.name)

    def __call__(self, fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner

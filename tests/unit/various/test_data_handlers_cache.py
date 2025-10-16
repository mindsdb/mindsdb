import time
import threading

from mindsdb.interfaces.database.data_handlers_cache import HandlersCache, HandlersCacheRecord
from mindsdb.utilities.context import context as ctx


class MockDatabaseHandler:
    """Mock database handler for testing"""

    def __init__(self, name: str, thread_safe: bool = True):
        self.name = name
        self.thread_safe = thread_safe
        self.is_connected = False

    def connect(self):
        self.is_connected = True

    def disconnect(self):
        self.is_connected = False


class TestHandlersCache:
    def test_record(self):
        """Test HandlersCacheRecord"""
        record = HandlersCacheRecord(handler=MockDatabaseHandler("test_handler"), expired_at=time.time() + 60)

        assert record.expired is False
        record.expired_at = time.time() - 1
        assert record.expired is True

        assert record.handler.is_connected is False
        record.connect()
        assert record.handler.is_connected is True

        assert record.has_references is False
        ref = record.handler  # noqa
        assert record.has_references is True
        ref = None  # noqa
        assert record.has_references is False

    def test_cache(self):
        cache = HandlersCache(clean_timeout=0.1)

        def first_key():
            try:
                return list(cache.handlers.keys())[0]
            except Exception:
                return None

        assert len(cache.handlers) == 0
        assert cache.cleaner_thread is None

        cache.set(MockDatabaseHandler("test_handler_a", thread_safe=True))

        assert len(cache.handlers) == 1
        assert cache.cleaner_thread is not None

        cache.set(MockDatabaseHandler("test_handler_a", thread_safe=True))

        assert len(cache.handlers) == 1
        assert first_key()[2] == 0  # Thread id for thread safe handler
        assert len(cache.handlers[first_key()]) == 2

        handler_1 = cache.get("test_handler_a")
        handler_2 = cache.get("test_handler_a")
        handler_3 = cache.get("test_handler_a")
        assert handler_1 is not None
        assert handler_2 is not None
        assert handler_3 is None
        assert id(handler_1) != id(handler_2)

        # release handlers and try to get again
        handler_1 = None
        handler_2 = None
        handler_1 = cache.get("test_handler_a")
        assert handler_1 is not None

        # mark both as expired, and check that only one deleted (handler_1 has references, therefore is in use)
        for handler_key in cache.handlers:
            for record in cache.handlers[handler_key]:
                record.expired_at = time.time() - 1

        time.sleep(0.3)
        assert len(cache.handlers[first_key()]) == 1
        handler_1 = None
        time.sleep(0.3)
        assert len(cache.handlers) == 0

        cache.set(MockDatabaseHandler("test_handler_a", thread_safe=True))
        cache.set(MockDatabaseHandler("test_handler_b", thread_safe=True))
        cache.set(MockDatabaseHandler("test_handler_c", thread_safe=False))

        # get non-thread-safe record from different threads
        # and set thread-safe handler in another thread
        def thread_handler():
            ctx.set_default()
            handler = cache.get("test_handler_c")
            assert handler is None
            cache.set(MockDatabaseHandler("test_handler_c", thread_safe=False))
            cache.set(MockDatabaseHandler("test_handler_d", thread_safe=True))
            handler = cache.get("test_handler_c")
            assert handler is not None

        t = threading.Thread(target=thread_handler)
        t.start()
        t.join()

        # should be 2 keys for thread_c, for different threads
        keys_count = sum([1 for key in cache.handlers if key[0] == "test_handler_c"])
        assert keys_count == 2

        # only one handler_c can be returned - from current thread
        handler = cache.get("test_handler_c")
        assert handler is not None
        handler = cache.get("test_handler_c")
        assert handler is None

        # thread-save handler created in another thread
        handler = cache.get("test_handler_d")
        assert handler is not None

        # check that handler b can be retrieved once, while there are another handlers records
        handler_b_1 = cache.get("test_handler_b")
        handler_b_2 = cache.get("test_handler_b")
        assert handler_b_1 is not None
        assert handler_b_2 is None

        # expire all handlers and check that they are cleared
        handler = None
        handler_b_1 = None
        handler_b_2 = None
        assert len(cache.handlers) != 0
        for handler_key in cache.handlers:
            for record in cache.handlers[handler_key]:
                record.expired_at = time.time() - 1
        time.sleep(0.3)
        assert len(cache.handlers) == 0

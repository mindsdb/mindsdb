import sys
import time
import threading
from dataclasses import dataclass

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities.context import context as ctx


@dataclass(kw_only=True, slots=True)
class HandlersCacheRecord:
    handler: DatabaseHandler
    expired_at: float

    @property
    def expired(self):
        return self.expired_at < time.time()

    @property
    def has_references(self):
        return sys.getrefcount(self.handler) > 2


class HandlersCache:
    """Cache for data handlers that keep connections opened during ttl time from handler last use"""

    def __init__(self, ttl: int = 60):
        """init cache

        Args:
            ttl (int): time to live (in seconds) for record in cache
        """
        self.ttl = ttl
        self.handlers = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self.cleaner_thread = None

    def __del__(self):
        self._stop_clean()

    def _start_clean(self) -> None:
        """start worker that close connections after ttl expired"""
        if isinstance(self.cleaner_thread, threading.Thread) and self.cleaner_thread.is_alive():
            return
        self._stop_event.clear()
        self.cleaner_thread = threading.Thread(target=self._clean, name="HandlersCache.clean")
        self.cleaner_thread.daemon = True
        self.cleaner_thread.start()

    def _stop_clean(self) -> None:
        """stop clean worker"""
        self._stop_event.set()

    def set(self, handler: DatabaseHandler):
        """add (or replace) handler in cache

        NOTE: If the handler is not thread-safe, then use a lock when making connection. Otherwise, make connection in
        the same thread without using a lock to speed up parallel queries. (They don't need to wait for a connection in
        another thread.)

        Args:
            handler (DatabaseHandler)
        """
        thread_safe = getattr(handler, "thread_safe", True)
        with self._lock:
            try:
                # If the handler is defined to be thread safe, set 0 as the last element of the key, otherwise set the thrad ID.
                key = (
                    handler.name,
                    ctx.company_id,
                    0 if thread_safe else threading.get_native_id(),
                )
                self.handlers[key] = HandlersCacheRecord(
                    handler=handler,
                    expired_at=time.time() + self.ttl
                )
                if thread_safe:
                    handler.connect()
            except Exception:
                pass
            self._start_clean()
        try:
            if not thread_safe:
                handler.connect()
        except Exception:
            pass

    def _get_cache_record(self, name: str) -> tuple[dict | None, str]:
        # If the handler is not thread safe, the thread ID will be assigned to the last element of the key.
        key = (name, ctx.company_id, 0)
        if key not in self.handlers:
            key = (name, ctx.company_id, threading.get_native_id())
        return self.handlers.get(key), key

    def get(self, name: str) -> DatabaseHandler | None:
        """get handler from cache by name

        Args:
            name (str): handler name

        Returns:
            DatabaseHandler
        """
        with self._lock:
            record, _ = self._get_cache_record(name)
            if (
                record is None
                or record.expired
                or record.has_references
            ):
                return None
            record.expired_at = time.time() + self.ttl
            return record.handler

    def delete(self, name: str) -> None:
        """delete handler from cache

        Args:
            name (str): handler name
        """
        with self._lock:
            record, key = self._get_cache_record(name)
            if record:
                try:
                    record.handler.disconnect()
                except Exception:
                    pass
                del self.handlers[key]
            if len(self.handlers) == 0:
                self._stop_clean()

    def _clean(self) -> None:
        """worker that delete from cache handlers that was not in use for ttl"""
        while self._stop_event.wait(timeout=3) is False:
            with self._lock:
                for key in list(self.handlers.keys()):
                    if (
                        self.handlers[key].expired
                        and self.handlers[key].has_references is False
                    ):
                        try:
                            self.handlers[key].handler.disconnect()
                        except Exception:
                            pass
                        del self.handlers[key]
                if len(self.handlers) == 0:
                    self._stop_event.set()

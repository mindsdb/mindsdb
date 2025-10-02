import sys
import time
import threading
from dataclasses import dataclass, field
from collections import defaultdict

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log

logger = log.getLogger(__name__)


@dataclass(kw_only=True, slots=True)
class HandlersCacheRecord:
    """Record for a handler in the cache

    Args:
        handler (DatabaseHandler): handler instance
        expired_at (float): time when the handler will be expired
    """

    handler: DatabaseHandler
    expired_at: float
    connect_attempt_done: threading.Event = field(default_factory=threading.Event)

    @property
    def expired(self) -> bool:
        """check if the handler is expired

        Returns:
            bool: True if the handler is expired, False otherwise
        """
        return self.expired_at < time.time()

    @property
    def has_references(self) -> bool:
        """check if the handler has references

        Returns:
            bool: True if the handler has references, False otherwise
        """
        return sys.getrefcount(self.handler) > 2

    def connect(self) -> None:
        """connect to the handler"""
        try:
            if not self.handler.is_connected:
                self.handler.connect()
        except Exception:
            logger.warning(f"Error connecting to handler: {self.handler.name}", exc_info=True)
        finally:
            self.connect_attempt_done.set()


class HandlersCache:
    """Cache for data handlers that keep connections opened during ttl time from handler last use"""

    def __init__(self, ttl: int = 60, clean_timeout: float = 3):
        """init cache

        Args:
            ttl (int): time to live (in seconds) for record in cache
            clean_timeout (float): interval between cleanups of expired handlers
        """
        self.ttl: int = ttl
        self._clean_timeout: int = clean_timeout
        self.handlers: dict[str, list[HandlersCacheRecord]] = defaultdict(list)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self.cleaner_thread = None

    def __del__(self):
        self._stop_clean()

    def _start_clean(self) -> None:
        """start worker that close connections after ttl expired"""
        if isinstance(self.cleaner_thread, threading.Thread) and self.cleaner_thread.is_alive():
            return
        with self._lock:
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
                record = HandlersCacheRecord(handler=handler, expired_at=time.time() + self.ttl)
                self.handlers[key].append(record)
            except Exception:
                logger.warning("Error setting data handler cache record:", exc_info=True)
                return
        self._start_clean()
        record.connect()

    def _get_cache_records(self, name: str) -> tuple[list[HandlersCacheRecord] | None, str]:
        """get cache records by name

        Args:
            name (str): handler name

        Returns:
            tuple[list[HandlersCacheRecord] | None, str]: cache records and key of the handler in cache
        """
        # If the handler is not thread safe, the thread ID will be assigned to the last element of the key.
        key = (name, ctx.company_id, 0)
        if key not in self.handlers:
            key = (name, ctx.company_id, threading.get_native_id())
        return self.handlers.get(key, []), key

    def get(self, name: str) -> DatabaseHandler | None:
        """get handler from cache by name

        Args:
            name (str): handler name

        Returns:
            DatabaseHandler
        """
        with self._lock:
            records, _ = self._get_cache_records(name)
            for record in records:
                if record.expired is False and record.has_references is False:
                    record.expired_at = time.time() + self.ttl
                    if record.connect_attempt_done.wait(timeout=10) is False:
                        logger.warning(f"Handler's connection attempt has not finished in 10s: {record.handler.name}")
                    return record.handler
            return None

    def delete(self, name: str) -> None:
        """delete handler from cache

        Args:
            name (str): handler name
        """
        with self._lock:
            records, key = self._get_cache_records(name)
            if len(records) > 0:
                del self.handlers[key]
            for record in records:
                try:
                    record.handler.disconnect()
                except Exception:
                    logger.debug("Error disconnecting data handler:", exc_info=True)

            if len(self.handlers) == 0:
                self._stop_clean()

    def _clean(self) -> None:
        """worker that delete from cache handlers that was not in use for ttl"""
        while self._stop_event.wait(timeout=self._clean_timeout) is False:
            with self._lock:
                for key in list(self.handlers.keys()):
                    active_handlers_list = []
                    for record in self.handlers[key]:
                        if record.expired and record.has_references is False:
                            try:
                                record.handler.disconnect()
                            except Exception:
                                logger.debug("Error disconnecting data handler:", exc_info=True)
                        else:
                            active_handlers_list.append(record)
                    if len(active_handlers_list) > 0:
                        self.handlers[key] = active_handlers_list
                    else:
                        del self.handlers[key]

                if len(self.handlers) == 0:
                    self._stop_event.set()

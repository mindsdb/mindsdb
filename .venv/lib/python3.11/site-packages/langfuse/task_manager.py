"""@private"""

import atexit
import json
import logging
import queue
import threading
from queue import Empty, Queue
import time
from typing import List, Any
from datetime import datetime, timezone
import typing


try:
    import pydantic.v1 as pydantic  # type: ignore
except ImportError:
    import pydantic  # type: ignore


import backoff

from langfuse.request import LangfuseClient
from langfuse.serializer import EventSerializer

# largest message size in db is 331_000 bytes right now
MAX_MSG_SIZE = 1_000_000

# https://vercel.com/docs/functions/serverless-functions/runtimes#request-body-size
# The maximum payload size for the request body or the response body of a Serverless Function is 4.5 MB
# 4_500_000 Bytes = 4.5 MB
# configured to be 3 MB to be safe

BATCH_SIZE_LIMIT = 2_500_000


class LangfuseMetadata(pydantic.BaseModel):
    batch_size: int
    sdk_integration: typing.Optional[str] = None
    sdk_name: str = None
    sdk_version: str = None
    public_key: str = None


class Consumer(threading.Thread):
    _log = logging.getLogger("langfuse")
    _queue: Queue
    _identifier: int
    _client: LangfuseClient
    _flush_at: int
    _flush_interval: float
    _max_retries: int
    _public_key: str
    _sdk_name: str
    _sdk_version: str
    _sdk_integration: str

    def __init__(
        self,
        queue: Queue,
        identifier: int,
        client: LangfuseClient,
        flush_at: int,
        flush_interval: float,
        max_retries: int,
        public_key: str,
        sdk_name: str,
        sdk_version: str,
        sdk_integration: str,
    ):
        """Create a consumer thread."""
        threading.Thread.__init__(self)
        # Make consumer a daemon thread so that it doesn't block program exit
        self.daemon = True
        self._queue = queue
        # It's important to set running in the constructor: if we are asked to
        # pause immediately after construction, we might set running to True in
        # run() *after* we set it to False in pause... and keep running
        # forever.
        self.running = True
        self._identifier = identifier
        self._client = client
        self._flush_at = flush_at
        self._flush_interval = flush_interval
        self._max_retries = max_retries
        self._public_key = public_key
        self._sdk_name = sdk_name
        self._sdk_version = sdk_version
        self._sdk_integration = sdk_integration

    def _next(self):
        """Return the next batch of items to upload."""
        queue = self._queue
        items = []

        start_time = time.monotonic()
        total_size = 0

        while len(items) < self._flush_at:
            elapsed = time.monotonic() - start_time
            if elapsed >= self._flush_interval:
                break
            try:
                item = queue.get(block=True, timeout=self._flush_interval - elapsed)
                item_size = len(json.dumps(item, cls=EventSerializer).encode())
                self._log.debug(f"item size {item_size}")
                if item_size > MAX_MSG_SIZE:
                    self._log.warning(
                        "Item exceeds size limit (size: %s), dropping input/output of item.",
                        item_size,
                    )

                    # for large events, drop input / output within the body
                    if "body" in item and "input" in item["body"]:
                        item["body"]["input"] = None
                    if "body" in item and "output" in item["body"]:
                        item["body"]["output"] = None

                    # if item does not have body or input/output fields, drop the event
                    if "body" not in item or (
                        "input" not in item["body"] and "output" not in item["body"]
                    ):
                        self._log.warning(
                            "Item does not have body or input/output fields, dropping item."
                        )
                        self._queue.task_done()
                        continue

                    # need to calculate the size again after dropping input/output
                    item_size = len(json.dumps(item, cls=EventSerializer).encode())
                    self._log.debug(
                        f"item size after dropping input/output {item_size}"
                    )

                items.append(item)
                total_size += item_size
                if total_size >= BATCH_SIZE_LIMIT:
                    self._log.debug("hit batch size limit (size: %d)", total_size)
                    break

            except Empty:
                break
        self._log.debug("~%d items in the Langfuse queue", self._queue.qsize())

        return items

    def run(self):
        """Runs the consumer."""
        self._log.debug("consumer is running...")
        while self.running:
            self.upload()

    def upload(self):
        """Upload the next batch of items, return whether successful."""
        batch = self._next()
        if len(batch) == 0:
            return

        try:
            self._upload_batch(batch)
        except Exception as e:
            self._log.exception("error uploading: %s", e)
        finally:
            # mark items as acknowledged from queue
            for _ in batch:
                self._queue.task_done()

    def pause(self):
        """Pause the consumer."""
        self.running = False

    def _upload_batch(self, batch: List[Any]):
        self._log.debug("uploading batch of %d items", len(batch))

        metadata = LangfuseMetadata(
            batch_size=len(batch),
            sdk_integration=self._sdk_integration,
            sdk_name=self._sdk_name,
            sdk_version=self._sdk_version,
            public_key=self._public_key,
        ).dict()

        @backoff.on_exception(backoff.expo, Exception, max_tries=self._max_retries)
        def execute_task_with_backoff(batch: List[Any]):
            return self._client.batch_post(batch=batch, metadata=metadata)

        execute_task_with_backoff(batch)
        self._log.debug("successfully uploaded batch of %d items", len(batch))


class TaskManager(object):
    _log = logging.getLogger("langfuse")
    _consumers: List[Consumer]
    _enabled: bool
    _threads: int
    _max_task_queue_size: int
    _queue: Queue
    _client: LangfuseClient
    _flush_at: int
    _flush_interval: float
    _max_retries: int
    _public_key: str
    _sdk_name: str
    _sdk_version: str
    _sdk_integration: str

    def __init__(
        self,
        client: LangfuseClient,
        flush_at: int,
        flush_interval: float,
        max_retries: int,
        threads: int,
        public_key: str,
        sdk_name: str,
        sdk_version: str,
        sdk_integration: str,
        enabled: bool = True,
        max_task_queue_size: int = 100_000,
    ):
        self._max_task_queue_size = max_task_queue_size
        self._threads = threads
        self._queue = queue.Queue(self._max_task_queue_size)
        self._consumers = []
        self._client = client
        self._flush_at = flush_at
        self._flush_interval = flush_interval
        self._max_retries = max_retries
        self._public_key = public_key
        self._sdk_name = sdk_name
        self._sdk_version = sdk_version
        self._sdk_integration = sdk_integration
        self._enabled = enabled

        self.init_resources()

        # cleans up when the python interpreter closes
        atexit.register(self.join)

    def init_resources(self):
        for i in range(self._threads):
            consumer = Consumer(
                queue=self._queue,
                identifier=i,
                client=self._client,
                flush_at=self._flush_at,
                flush_interval=self._flush_interval,
                max_retries=self._max_retries,
                public_key=self._public_key,
                sdk_name=self._sdk_name,
                sdk_version=self._sdk_version,
                sdk_integration=self._sdk_integration,
            )
            consumer.start()
            self._consumers.append(consumer)

    def add_task(self, event: dict):
        if not self._enabled:
            return

        try:
            json.dumps(event, cls=EventSerializer)
            event["timestamp"] = datetime.utcnow().replace(tzinfo=timezone.utc)

            self._queue.put(event, block=False)
        except queue.Full:
            self._log.warning("analytics-python queue is full")
            return False
        except Exception as e:
            self._log.exception(f"Exception in adding task {e}")

            return False

    def flush(self):
        """Forces a flush from the internal queue to the server"""
        self._log.debug("flushing queue")
        queue = self._queue
        size = queue.qsize()
        queue.join()
        # Note that this message may not be precise, because of threading.
        self._log.debug("successfully flushed about %s items.", size)

    def join(self):
        """Ends the consumer threads once the queue is empty.
        Blocks execution until finished
        """
        self._log.debug(f"joining {len(self._consumers)} consumer threads")
        for consumer in self._consumers:
            consumer.pause()
            try:
                consumer.join()
            except RuntimeError:
                # consumer thread has not started
                pass

            self._log.debug(f"consumer thread {consumer._identifier} joined")

    def shutdown(self):
        """Flush all messages and cleanly shutdown the client"""
        self._log.debug("shutdown initiated")

        self.flush()
        self.join()

        self._log.debug("shutdown completed")

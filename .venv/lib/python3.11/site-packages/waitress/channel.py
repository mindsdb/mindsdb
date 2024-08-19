##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import socket
import threading
import time
import traceback

from waitress.buffers import OverflowableBuffer, ReadOnlyFileBasedBuffer
from waitress.parser import HTTPRequestParser
from waitress.task import ErrorTask, WSGITask
from waitress.utilities import InternalServerError

from . import wasyncore


class ClientDisconnected(Exception):
    """Raised when attempting to write to a closed socket."""


class HTTPChannel(wasyncore.dispatcher):
    """
    Setting self.requests = [somerequest] prevents more requests from being
    received until the out buffers have been flushed.

    Setting self.requests = [] allows more requests to be received.
    """

    task_class = WSGITask
    error_task_class = ErrorTask
    parser_class = HTTPRequestParser

    # A request that has not been received yet completely is stored here
    request = None
    last_activity = 0  # Time of last activity
    will_close = False  # set to True to close the socket.
    close_when_flushed = False  # set to True to close the socket when flushed
    sent_continue = False  # used as a latch after sending 100 continue
    total_outbufs_len = 0  # total bytes ready to send
    current_outbuf_count = 0  # total bytes written to current outbuf

    #
    # ASYNCHRONOUS METHODS (including __init__)
    #

    def __init__(self, server, sock, addr, adj, map=None):
        self.server = server
        self.adj = adj
        self.outbufs = [OverflowableBuffer(adj.outbuf_overflow)]
        self.creation_time = self.last_activity = time.time()
        self.sendbuf_len = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)

        # requests_lock used to push/pop requests and modify the request that is
        # currently being created
        self.requests_lock = threading.Lock()
        # outbuf_lock used to access any outbuf (expected to use an RLock)
        self.outbuf_lock = threading.Condition()

        wasyncore.dispatcher.__init__(self, sock, map=map)

        # Don't let wasyncore.dispatcher throttle self.addr on us.
        self.addr = addr
        self.requests = []

    def check_client_disconnected(self):
        """
        This method is inserted into the environment of any created task so it
        may occasionally check if the client has disconnected and interrupt
        execution.
        """

        return not self.connected

    def writable(self):
        # if there's data in the out buffer or we've been instructed to close
        # the channel (possibly by our server maintenance logic), run
        # handle_write

        return self.total_outbufs_len or self.will_close or self.close_when_flushed

    def handle_write(self):
        # Precondition: there's data in the out buffer to be sent, or
        # there's a pending will_close request

        if not self.connected:
            # we dont want to close the channel twice

            return

        # try to flush any pending output

        if not self.requests:
            # 1. There are no running tasks, so we don't need to try to lock
            #    the outbuf before sending
            # 2. The data in the out buffer should be sent as soon as possible
            #    because it's either data left over from task output
            #    or a 100 Continue line sent within "received".
            flush = self._flush_some
        elif self.total_outbufs_len >= self.adj.send_bytes:
            # 1. There's a running task, so we need to try to lock
            #    the outbuf before sending
            # 2. Only try to send if the data in the out buffer is larger
            #    than self.adj_bytes to avoid TCP fragmentation
            flush = self._flush_some_if_lockable
        else:
            # 1. There's not enough data in the out buffer to bother to send
            #    right now.
            flush = None

        self._flush_exception(flush)

        if self.close_when_flushed and not self.total_outbufs_len:
            self.close_when_flushed = False
            self.will_close = True

        if self.will_close:
            self.handle_close()

    def _flush_exception(self, flush, do_close=True):
        if flush:
            try:
                return (flush(do_close=do_close), False)
            except OSError:
                if self.adj.log_socket_errors:
                    self.logger.exception("Socket error")
                self.will_close = True

                return (False, True)
            except Exception:  # pragma: nocover
                self.logger.exception("Unexpected exception when flushing")
                self.will_close = True

                return (False, True)

    def readable(self):
        # We might want to read more requests. We can only do this if:
        # 1. We're not already about to close the connection.
        # 2. We're not waiting to flush remaining data before closing the
        #    connection
        # 3. There are not too many tasks already queued
        # 4. There's no data in the output buffer that needs to be sent
        #    before we potentially create a new task.

        return not (
            self.will_close
            or self.close_when_flushed
            or len(self.requests) > self.adj.channel_request_lookahead
            or self.total_outbufs_len
        )

    def handle_read(self):
        try:
            data = self.recv(self.adj.recv_bytes)
        except OSError:
            if self.adj.log_socket_errors:
                self.logger.exception("Socket error")
            self.handle_close()

            return

        if data:
            self.last_activity = time.time()
            self.received(data)
        else:
            # Client disconnected.
            self.connected = False

    def send_continue(self):
        """
        Send a 100-Continue header to the client. This is either called from
        receive (if no requests are running and the client expects it) or at
        the end of service (if no more requests are queued and a request has
        been read partially that expects it).
        """
        self.request.expect_continue = False
        outbuf_payload = b"HTTP/1.1 100 Continue\r\n\r\n"
        num_bytes = len(outbuf_payload)
        with self.outbuf_lock:
            self.outbufs[-1].append(outbuf_payload)
            self.current_outbuf_count += num_bytes
            self.total_outbufs_len += num_bytes
            self.sent_continue = True
            self._flush_some()
        self.request.completed = False

    def received(self, data):
        """
        Receives input asynchronously and assigns one or more requests to the
        channel.
        """

        if not data:
            return False

        with self.requests_lock:
            while data:
                if self.request is None:
                    self.request = self.parser_class(self.adj)
                n = self.request.received(data)

                # if there are requests queued, we can not send the continue
                # header yet since the responses need to be kept in order

                if (
                    self.request.expect_continue
                    and self.request.headers_finished
                    and not self.requests
                    and not self.sent_continue
                ):
                    self.send_continue()

                if self.request.completed:
                    # The request (with the body) is ready to use.
                    self.sent_continue = False

                    if not self.request.empty:
                        self.requests.append(self.request)

                        if len(self.requests) == 1:
                            # self.requests was empty before so the main thread
                            # is in charge of starting the task. Otherwise,
                            # service() will add a new task after each request
                            # has been processed
                            self.server.add_task(self)
                    self.request = None

                if n >= len(data):
                    break
                data = data[n:]

        return True

    def _flush_some_if_lockable(self, do_close=True):
        # Since our task may be appending to the outbuf, we try to acquire
        # the lock, but we don't block if we can't.

        if self.outbuf_lock.acquire(False):
            try:
                self._flush_some(do_close=do_close)

                if self.total_outbufs_len < self.adj.outbuf_high_watermark:
                    self.outbuf_lock.notify()
            finally:
                self.outbuf_lock.release()

    def _flush_some(self, do_close=True):
        # Send as much data as possible to our client

        sent = 0
        dobreak = False

        while True:
            outbuf = self.outbufs[0]
            # use outbuf.__len__ rather than len(outbuf) FBO of not getting
            # OverflowError on 32-bit Python
            outbuflen = outbuf.__len__()

            while outbuflen > 0:
                chunk = outbuf.get(self.sendbuf_len)
                num_sent = self.send(chunk, do_close=do_close)

                if num_sent:
                    outbuf.skip(num_sent, True)
                    outbuflen -= num_sent
                    sent += num_sent
                    self.total_outbufs_len -= num_sent
                else:
                    # failed to write anything, break out entirely
                    dobreak = True

                    break
            else:
                # self.outbufs[-1] must always be a writable outbuf

                if len(self.outbufs) > 1:
                    toclose = self.outbufs.pop(0)
                    try:
                        toclose.close()
                    except Exception:
                        self.logger.exception("Unexpected error when closing an outbuf")
                else:
                    # caught up, done flushing for now
                    dobreak = True

            if dobreak:
                break

        if sent:
            self.last_activity = time.time()

            return True

        return False

    def handle_close(self):
        with self.outbuf_lock:
            for outbuf in self.outbufs:
                try:
                    outbuf.close()
                except Exception:
                    self.logger.exception(
                        "Unknown exception while trying to close outbuf"
                    )
            self.total_outbufs_len = 0
            self.connected = False
            self.outbuf_lock.notify()
        wasyncore.dispatcher.close(self)

    def add_channel(self, map=None):
        """See wasyncore.dispatcher

        This hook keeps track of opened channels.
        """
        wasyncore.dispatcher.add_channel(self, map)
        self.server.active_channels[self._fileno] = self

    def del_channel(self, map=None):
        """See wasyncore.dispatcher

        This hook keeps track of closed channels.
        """
        fd = self._fileno  # next line sets this to None
        wasyncore.dispatcher.del_channel(self, map)
        ac = self.server.active_channels

        if fd in ac:
            del ac[fd]

    #
    # SYNCHRONOUS METHODS
    #

    def write_soon(self, data):
        if not self.connected:
            # if the socket is closed then interrupt the task so that it
            # can cleanup possibly before the app_iter is exhausted
            raise ClientDisconnected

        if data:
            # the async mainloop might be popping data off outbuf; we can
            # block here waiting for it because we're in a task thread
            with self.outbuf_lock:
                self._flush_outbufs_below_high_watermark()

                if not self.connected:
                    raise ClientDisconnected
                num_bytes = len(data)

                if data.__class__ is ReadOnlyFileBasedBuffer:
                    # they used wsgi.file_wrapper
                    self.outbufs.append(data)
                    nextbuf = OverflowableBuffer(self.adj.outbuf_overflow)
                    self.outbufs.append(nextbuf)
                    self.current_outbuf_count = 0
                else:
                    if self.current_outbuf_count >= self.adj.outbuf_high_watermark:
                        # rotate to a new buffer if the current buffer has hit
                        # the watermark to avoid it growing unbounded
                        nextbuf = OverflowableBuffer(self.adj.outbuf_overflow)
                        self.outbufs.append(nextbuf)
                        self.current_outbuf_count = 0
                    self.outbufs[-1].append(data)
                    self.current_outbuf_count += num_bytes
                self.total_outbufs_len += num_bytes

                if self.total_outbufs_len >= self.adj.send_bytes:
                    (flushed, exception) = self._flush_exception(
                        self._flush_some, do_close=False
                    )

                    if (
                        exception
                        or not flushed
                        or self.total_outbufs_len >= self.adj.send_bytes
                    ):
                        self.server.pull_trigger()

            return num_bytes

        return 0

    def _flush_outbufs_below_high_watermark(self):
        # check first to avoid locking if possible

        if self.total_outbufs_len > self.adj.outbuf_high_watermark:
            with self.outbuf_lock:
                (_, exception) = self._flush_exception(self._flush_some, do_close=False)

                if exception:
                    # An exception happened while flushing, wake up the main
                    # thread, then wait for it to decide what to do next
                    # (probably close the socket, and then just return)
                    self.server.pull_trigger()
                    self.outbuf_lock.wait()

                    return

                while (
                    self.connected
                    and self.total_outbufs_len > self.adj.outbuf_high_watermark
                ):
                    self.server.pull_trigger()
                    self.outbuf_lock.wait()

    def service(self):
        """Execute one request. If there are more, we add another task to the
        server at the end."""

        request = self.requests[0]

        if request.error:
            task = self.error_task_class(self, request)
        else:
            task = self.task_class(self, request)

        try:
            if self.connected:
                task.service()
            else:
                task.close_on_finish = True
        except ClientDisconnected:
            self.logger.info("Client disconnected while serving %s" % task.request.path)
            task.close_on_finish = True
        except Exception:
            self.logger.exception("Exception while serving %s" % task.request.path)

            if not task.wrote_header:
                if self.adj.expose_tracebacks:
                    body = traceback.format_exc()
                else:
                    body = "The server encountered an unexpected internal server error"
                req_version = request.version
                req_headers = request.headers
                err_request = self.parser_class(self.adj)
                err_request.error = InternalServerError(body)
                # copy some original request attributes to fulfill
                # HTTP 1.1 requirements
                err_request.version = req_version
                try:
                    err_request.headers["CONNECTION"] = req_headers["CONNECTION"]
                except KeyError:
                    pass
                task = self.error_task_class(self, err_request)
                try:
                    task.service()  # must not fail
                except ClientDisconnected:
                    task.close_on_finish = True
            else:
                task.close_on_finish = True

        if task.close_on_finish:
            with self.requests_lock:
                self.close_when_flushed = True

                for request in self.requests:
                    request.close()
                self.requests = []
        else:
            # before processing a new request, ensure there is not too
            # much data in the outbufs waiting to be flushed
            # NB: currently readable() returns False while we are
            # flushing data so we know no new requests will come in
            # that we need to account for, otherwise it'd be better
            # to do this check at the start of the request instead of
            # at the end to account for consecutive service() calls

            if len(self.requests) > 1:
                self._flush_outbufs_below_high_watermark()

            # this is a little hacky but basically it's forcing the
            # next request to create a new outbuf to avoid sharing
            # outbufs across requests which can cause outbufs to
            # not be deallocated regularly when a connection is open
            # for a long time

            if self.current_outbuf_count > 0:
                self.current_outbuf_count = self.adj.outbuf_high_watermark

            request.close()

            # Add new task to process the next request
            with self.requests_lock:
                self.requests.pop(0)

                if self.connected and self.requests:
                    self.server.add_task(self)
                elif (
                    self.connected
                    and self.request is not None
                    and self.request.expect_continue
                    and self.request.headers_finished
                    and not self.sent_continue
                ):
                    # A request waits for a signal to continue, but we could
                    # not send it until now because requests were being
                    # processed and the output needs to be kept in order
                    self.send_continue()

        if self.connected:
            self.server.pull_trigger()

        self.last_activity = time.time()

    def cancel(self):
        """Cancels all pending / active requests"""
        self.will_close = True
        self.connected = False
        self.last_activity = time.time()
        self.requests = []

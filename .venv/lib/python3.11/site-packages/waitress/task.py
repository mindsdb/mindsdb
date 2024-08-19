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

from collections import deque
import socket
import sys
import threading
import time

from .buffers import ReadOnlyFileBasedBuffer
from .utilities import build_http_date, logger, queue_logger

rename_headers = {  # or keep them without the HTTP_ prefix added
    "CONTENT_LENGTH": "CONTENT_LENGTH",
    "CONTENT_TYPE": "CONTENT_TYPE",
}

hop_by_hop = frozenset(
    (
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    )
)


class ThreadedTaskDispatcher:
    """A Task Dispatcher that creates a thread for each task."""

    stop_count = 0  # Number of threads that will stop soon.
    active_count = 0  # Number of currently active threads
    logger = logger
    queue_logger = queue_logger

    def __init__(self):
        self.threads = set()
        self.queue = deque()
        self.lock = threading.Lock()
        self.queue_cv = threading.Condition(self.lock)
        self.thread_exit_cv = threading.Condition(self.lock)

    def start_new_thread(self, target, thread_no):
        t = threading.Thread(
            target=target, name=f"waitress-{thread_no}", args=(thread_no,)
        )
        t.daemon = True
        t.start()

    def handler_thread(self, thread_no):
        while True:
            with self.lock:
                while not self.queue and self.stop_count == 0:
                    # Mark ourselves as idle before waiting to be
                    # woken up, then we will once again be active
                    self.active_count -= 1
                    self.queue_cv.wait()
                    self.active_count += 1

                if self.stop_count > 0:
                    self.active_count -= 1
                    self.stop_count -= 1
                    self.threads.discard(thread_no)
                    self.thread_exit_cv.notify()
                    break

                task = self.queue.popleft()
            try:
                task.service()
            except BaseException:
                self.logger.exception("Exception when servicing %r", task)

    def set_thread_count(self, count):
        with self.lock:
            threads = self.threads
            thread_no = 0
            running = len(threads) - self.stop_count
            while running < count:
                # Start threads.
                while thread_no in threads:
                    thread_no = thread_no + 1
                threads.add(thread_no)
                running += 1
                self.start_new_thread(self.handler_thread, thread_no)
                self.active_count += 1
                thread_no = thread_no + 1
            if running > count:
                # Stop threads.
                self.stop_count += running - count
                self.queue_cv.notify_all()

    def add_task(self, task):
        with self.lock:
            self.queue.append(task)
            self.queue_cv.notify()
            queue_size = len(self.queue)
            idle_threads = len(self.threads) - self.stop_count - self.active_count
            if queue_size > idle_threads:
                self.queue_logger.warning(
                    "Task queue depth is %d", queue_size - idle_threads
                )

    def shutdown(self, cancel_pending=True, timeout=5):
        self.set_thread_count(0)
        # Ensure the threads shut down.
        threads = self.threads
        expiration = time.time() + timeout
        with self.lock:
            while threads:
                if time.time() >= expiration:
                    self.logger.warning("%d thread(s) still running", len(threads))
                    break
                self.thread_exit_cv.wait(0.1)
            if cancel_pending:
                # Cancel remaining tasks.
                queue = self.queue
                if len(queue) > 0:
                    self.logger.warning("Canceling %d pending task(s)", len(queue))
                while queue:
                    task = queue.popleft()
                    task.cancel()
                self.queue_cv.notify_all()
                return True
        return False


class Task:
    close_on_finish = False
    status = "200 OK"
    wrote_header = False
    start_time = 0
    content_length = None
    content_bytes_written = 0
    logged_write_excess = False
    logged_write_no_body = False
    complete = False
    chunked_response = False
    logger = logger

    def __init__(self, channel, request):
        self.channel = channel
        self.request = request
        self.response_headers = []
        version = request.version
        if version not in ("1.0", "1.1"):
            # fall back to a version we support.
            version = "1.0"
        self.version = version

    def service(self):
        try:
            self.start()
            self.execute()
            self.finish()
        except OSError:
            self.close_on_finish = True
            if self.channel.adj.log_socket_errors:
                raise

    @property
    def has_body(self):
        return not (
            self.status.startswith("1")
            or self.status.startswith("204")
            or self.status.startswith("304")
        )

    def set_close_on_finish(self) -> None:
        # if headers have not been written yet, tell the remote
        # client we are closing the connection
        if not self.wrote_header:
            connection_close_header = None
            for headername, headerval in self.response_headers:
                if headername.capitalize() == "Connection":
                    connection_close_header = headerval.lower()
            if connection_close_header is None:
                self.response_headers.append(("Connection", "close"))
        self.close_on_finish = True

    def build_response_header(self):
        version = self.version
        # Figure out whether the connection should be closed.
        connection = self.request.headers.get("CONNECTION", "").lower()
        response_headers = []
        content_length_header = None
        date_header = None
        server_header = None

        for headername, headerval in self.response_headers:
            headername = "-".join([x.capitalize() for x in headername.split("-")])

            if headername == "Content-Length":
                if self.has_body:
                    content_length_header = headerval
                else:
                    continue  # pragma: no cover

            if headername == "Date":
                date_header = headerval

            if headername == "Server":
                server_header = headerval

            # replace with properly capitalized version
            response_headers.append((headername, headerval))

        # Overwrite the response headers we have with normalized ones
        self.response_headers = response_headers

        if (
            content_length_header is None
            and self.content_length is not None
            and self.has_body
        ):
            content_length_header = str(self.content_length)
            self.response_headers.append(("Content-Length", content_length_header))

        if version == "1.0":
            if connection == "keep-alive":
                if not content_length_header:
                    self.set_close_on_finish()
                else:
                    self.response_headers.append(("Connection", "Keep-Alive"))
            else:
                self.set_close_on_finish()

        elif version == "1.1":
            if connection == "close":
                self.set_close_on_finish()

            if not content_length_header:
                # RFC 7230: MUST NOT send Transfer-Encoding or Content-Length
                # for any response with a status code of 1xx, 204 or 304.

                if self.has_body:
                    self.response_headers.append(("Transfer-Encoding", "chunked"))
                    self.chunked_response = True

                if not self.close_on_finish:
                    self.set_close_on_finish()

            # under HTTP 1.1 keep-alive is default, no need to set the header
        else:
            raise AssertionError("neither HTTP/1.0 or HTTP/1.1")

        # Set the Server and Date field, if not yet specified. This is needed
        # if the server is used as a proxy.
        ident = self.channel.server.adj.ident

        if not server_header:
            if ident:
                self.response_headers.append(("Server", ident))
        else:
            self.response_headers.append(("Via", ident or "waitress"))

        if not date_header:
            self.response_headers.append(("Date", build_http_date(self.start_time)))

        first_line = f"HTTP/{self.version} {self.status}"
        # NB: sorting headers needs to preserve same-named-header order
        # as per RFC 2616 section 4.2; thus the key=lambda x: x[0] here;
        # rely on stable sort to keep relative position of same-named headers
        next_lines = [
            "%s: %s" % hv for hv in sorted(self.response_headers, key=lambda x: x[0])
        ]
        lines = [first_line] + next_lines
        res = "%s\r\n\r\n" % "\r\n".join(lines)

        return res.encode("latin-1")

    def remove_content_length_header(self):
        response_headers = []

        for header_name, header_value in self.response_headers:
            if header_name.lower() == "content-length":
                continue  # pragma: nocover
            response_headers.append((header_name, header_value))

        self.response_headers = response_headers

    def start(self):
        self.start_time = time.time()

    def finish(self):
        if not self.wrote_header:
            self.write(b"")
        if self.chunked_response:
            # not self.write, it will chunk it!
            self.channel.write_soon(b"0\r\n\r\n")

    def write(self, data):
        if not self.complete:
            raise RuntimeError("start_response was not called before body written")
        channel = self.channel
        if not self.wrote_header:
            rh = self.build_response_header()
            channel.write_soon(rh)
            self.wrote_header = True

        if data and self.has_body:
            towrite = data
            cl = self.content_length
            if self.chunked_response:
                # use chunked encoding response
                towrite = hex(len(data))[2:].upper().encode("latin-1") + b"\r\n"
                towrite += data + b"\r\n"
            elif cl is not None:
                towrite = data[: cl - self.content_bytes_written]
                self.content_bytes_written += len(towrite)
                if towrite != data and not self.logged_write_excess:
                    self.logger.warning(
                        "application-written content exceeded the number of "
                        "bytes specified by Content-Length header (%s)" % cl
                    )
                    self.logged_write_excess = True
            if towrite:
                channel.write_soon(towrite)
        elif data:
            # Cheat, and tell the application we have written all of the bytes,
            # even though the response shouldn't have a body and we are
            # ignoring it entirely.
            self.content_bytes_written += len(data)

            if not self.logged_write_no_body:
                self.logger.warning(
                    "application-written content was ignored due to HTTP "
                    "response that may not contain a message-body: (%s)" % self.status
                )
                self.logged_write_no_body = True


class ErrorTask(Task):
    """An error task produces an error response"""

    complete = True

    def execute(self):
        ident = self.channel.server.adj.ident
        e = self.request.error
        status, headers, body = e.to_response(ident)
        self.status = status
        self.response_headers.extend(headers)
        self.set_close_on_finish()
        self.content_length = len(body)
        self.write(body)


class WSGITask(Task):
    """A WSGI task produces a response from a WSGI application."""

    environ = None

    def execute(self):
        environ = self.get_environment()

        def start_response(status, headers, exc_info=None):
            if self.complete and not exc_info:
                raise AssertionError(
                    "start_response called a second time without providing exc_info."
                )
            if exc_info:
                try:
                    if self.wrote_header:
                        # higher levels will catch and handle raised exception:
                        # 1. "service" method in task.py
                        # 2. "service" method in channel.py
                        # 3. "handler_thread" method in task.py
                        raise exc_info[1]
                    else:
                        # As per WSGI spec existing headers must be cleared
                        self.response_headers = []
                finally:
                    exc_info = None

            self.complete = True

            if status.__class__ is not str:
                raise AssertionError("status %s is not a string" % status)
            if "\n" in status or "\r" in status:
                raise ValueError(
                    "carriage return/line feed character present in status"
                )

            self.status = status

            # Prepare the headers for output
            for k, v in headers:
                if k.__class__ is not str:
                    raise AssertionError(
                        f"Header name {k!r} is not a string in {(k, v)!r}"
                    )
                if v.__class__ is not str:
                    raise AssertionError(
                        f"Header value {v!r} is not a string in {(k, v)!r}"
                    )

                if "\n" in v or "\r" in v:
                    raise ValueError(
                        "carriage return/line feed character present in header value"
                    )
                if "\n" in k or "\r" in k:
                    raise ValueError(
                        "carriage return/line feed character present in header name"
                    )

                kl = k.lower()
                if kl == "content-length":
                    self.content_length = int(v)
                elif kl in hop_by_hop:
                    raise AssertionError(
                        '%s is a "hop-by-hop" header; it cannot be used by '
                        "a WSGI application (see PEP 3333)" % k
                    )

            self.response_headers.extend(headers)

            # Return a method used to write the response data.
            return self.write

        # Call the application to handle the request and write a response
        app_iter = self.channel.server.application(environ, start_response)

        can_close_app_iter = True
        try:
            if app_iter.__class__ is ReadOnlyFileBasedBuffer:
                cl = self.content_length
                size = app_iter.prepare(cl)
                if size:
                    if cl != size:
                        if cl is not None:
                            self.remove_content_length_header()
                        self.content_length = size
                    self.write(b"")  # generate headers
                    # if the write_soon below succeeds then the channel will
                    # take over closing the underlying file via the channel's
                    # _flush_some or handle_close so we intentionally avoid
                    # calling close in the finally block
                    self.channel.write_soon(app_iter)
                    can_close_app_iter = False
                    return

            first_chunk_len = None
            for chunk in app_iter:
                if first_chunk_len is None:
                    first_chunk_len = len(chunk)
                    # Set a Content-Length header if one is not supplied.
                    # start_response may not have been called until first
                    # iteration as per PEP, so we must reinterrogate
                    # self.content_length here
                    if self.content_length is None:
                        app_iter_len = None
                        if hasattr(app_iter, "__len__"):
                            app_iter_len = len(app_iter)
                        if app_iter_len == 1:
                            self.content_length = first_chunk_len
                # transmit headers only after first iteration of the iterable
                # that returns a non-empty bytestring (PEP 3333)
                if chunk:
                    self.write(chunk)

            cl = self.content_length
            if cl is not None:
                if self.content_bytes_written != cl and self.request.command != "HEAD":
                    # close the connection so the client isn't sitting around
                    # waiting for more data when there are too few bytes
                    # to service content-length
                    self.set_close_on_finish()
                    if self.request.command != "HEAD":
                        self.logger.warning(
                            "application returned too few bytes (%s) "
                            "for specified Content-Length (%s) via app_iter",
                            self.content_bytes_written,
                            cl,
                        )
        finally:
            if can_close_app_iter and hasattr(app_iter, "close"):
                app_iter.close()

    def get_environment(self):
        """Returns a WSGI environment."""
        environ = self.environ
        if environ is not None:
            # Return the cached copy.
            return environ

        request = self.request
        path = request.path
        channel = self.channel
        server = channel.server
        url_prefix = server.adj.url_prefix

        if path.startswith("/"):
            # strip extra slashes at the beginning of a path that starts
            # with any number of slashes
            path = "/" + path.lstrip("/")

        if url_prefix:
            # NB: url_prefix is guaranteed by the configuration machinery to
            # be either the empty string or a string that starts with a single
            # slash and ends without any slashes
            if path == url_prefix:
                # if the path is the same as the url prefix, the SCRIPT_NAME
                # should be the url_prefix and PATH_INFO should be empty
                path = ""
            else:
                # if the path starts with the url prefix plus a slash,
                # the SCRIPT_NAME should be the url_prefix and PATH_INFO should
                # the value of path from the slash until its end
                url_prefix_with_trailing_slash = url_prefix + "/"
                if path.startswith(url_prefix_with_trailing_slash):
                    path = path[len(url_prefix) :]

        environ = {
            "REMOTE_ADDR": channel.addr[0],
            # Nah, we aren't actually going to look up the reverse DNS for
            # REMOTE_ADDR, but we will happily set this environment variable
            # for the WSGI application. Spec says we can just set this to
            # REMOTE_ADDR, so we do.
            "REMOTE_HOST": channel.addr[0],
            # try and set the REMOTE_PORT to something useful, but maybe None
            "REMOTE_PORT": str(channel.addr[1]),
            "REQUEST_METHOD": request.command.upper(),
            "SERVER_PORT": str(server.effective_port),
            "SERVER_NAME": server.server_name,
            "SERVER_SOFTWARE": server.adj.ident,
            "SERVER_PROTOCOL": "HTTP/%s" % self.version,
            "SCRIPT_NAME": url_prefix,
            "PATH_INFO": path,
            "REQUEST_URI": request.request_uri,
            "QUERY_STRING": request.query,
            "wsgi.url_scheme": request.url_scheme,
            # the following environment variables are required by the WSGI spec
            "wsgi.version": (1, 0),
            # apps should use the logging module
            "wsgi.errors": sys.stderr,
            "wsgi.multithread": True,
            "wsgi.multiprocess": False,
            "wsgi.run_once": False,
            "wsgi.input": request.get_body_stream(),
            "wsgi.file_wrapper": ReadOnlyFileBasedBuffer,
            "wsgi.input_terminated": True,  # wsgi.input is EOF terminated
        }

        for key, value in dict(request.headers).items():
            value = value.strip()
            mykey = rename_headers.get(key, None)
            if mykey is None:
                mykey = "HTTP_" + key
            if mykey not in environ:
                environ[mykey] = value

        # Insert a callable into the environment that allows the application to
        # check if the client disconnected. Only works with
        # channel_request_lookahead larger than 0.
        environ["waitress.client_disconnected"] = self.channel.check_client_disconnected

        # cache the environ for this request
        self.environ = environ
        return environ

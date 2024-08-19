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

import os
import os.path
import socket
import time

from waitress import trigger
from waitress.adjustments import Adjustments
from waitress.channel import HTTPChannel
from waitress.compat import IPPROTO_IPV6, IPV6_V6ONLY
from waitress.task import ThreadedTaskDispatcher
from waitress.utilities import cleanup_unix_socket

from . import wasyncore
from .proxy_headers import proxy_headers_middleware


def create_server(
    application,
    map=None,
    _start=True,  # test shim
    _sock=None,  # test shim
    _dispatcher=None,  # test shim
    **kw  # adjustments
):
    """
    if __name__ == '__main__':
        server = create_server(app)
        server.run()
    """
    if application is None:
        raise ValueError(
            'The "app" passed to ``create_server`` was ``None``.  You forgot '
            "to return a WSGI app within your application."
        )
    adj = Adjustments(**kw)

    if map is None:  # pragma: nocover
        map = {}

    dispatcher = _dispatcher
    if dispatcher is None:
        dispatcher = ThreadedTaskDispatcher()
        dispatcher.set_thread_count(adj.threads)

    if adj.unix_socket and hasattr(socket, "AF_UNIX"):
        sockinfo = (socket.AF_UNIX, socket.SOCK_STREAM, None, None)
        return UnixWSGIServer(
            application,
            map,
            _start,
            _sock,
            dispatcher=dispatcher,
            adj=adj,
            sockinfo=sockinfo,
        )

    effective_listen = []
    last_serv = None
    if not adj.sockets:
        for sockinfo in adj.listen:
            # When TcpWSGIServer is called, it registers itself in the map. This
            # side-effect is all we need it for, so we don't store a reference to
            # or return it to the user.
            last_serv = TcpWSGIServer(
                application,
                map,
                _start,
                _sock,
                dispatcher=dispatcher,
                adj=adj,
                sockinfo=sockinfo,
            )
            effective_listen.append(
                (last_serv.effective_host, last_serv.effective_port)
            )

    for sock in adj.sockets:
        sockinfo = (sock.family, sock.type, sock.proto, sock.getsockname())
        if sock.family == socket.AF_INET or sock.family == socket.AF_INET6:
            last_serv = TcpWSGIServer(
                application,
                map,
                _start,
                sock,
                dispatcher=dispatcher,
                adj=adj,
                bind_socket=False,
                sockinfo=sockinfo,
            )
            effective_listen.append(
                (last_serv.effective_host, last_serv.effective_port)
            )
        elif hasattr(socket, "AF_UNIX") and sock.family == socket.AF_UNIX:
            last_serv = UnixWSGIServer(
                application,
                map,
                _start,
                sock,
                dispatcher=dispatcher,
                adj=adj,
                bind_socket=False,
                sockinfo=sockinfo,
            )
            effective_listen.append(
                (last_serv.effective_host, last_serv.effective_port)
            )

    # We are running a single server, so we can just return the last server,
    # saves us from having to create one more object
    if len(effective_listen) == 1:
        # In this case we have no need to use a MultiSocketServer
        return last_serv

    log_info = last_serv.log_info
    # Return a class that has a utility function to print out the sockets it's
    # listening on, and has a .run() function. All of the TcpWSGIServers
    # registered themselves in the map above.
    return MultiSocketServer(map, adj, effective_listen, dispatcher, log_info)


# This class is only ever used if we have multiple listen sockets. It allows
# the serve() API to call .run() which starts the wasyncore loop, and catches
# SystemExit/KeyboardInterrupt so that it can attempt to cleanly shut down.
class MultiSocketServer:
    asyncore = wasyncore  # test shim

    def __init__(
        self,
        map=None,
        adj=None,
        effective_listen=None,
        dispatcher=None,
        log_info=None,
    ):
        self.adj = adj
        self.map = map
        self.effective_listen = effective_listen
        self.task_dispatcher = dispatcher
        self.log_info = log_info

    def print_listen(self, format_str):  # pragma: nocover
        for l in self.effective_listen:
            l = list(l)

            if ":" in l[0]:
                l[0] = f"[{l[0]}]"

            self.log_info(format_str.format(*l))

    def run(self):
        try:
            self.asyncore.loop(
                timeout=self.adj.asyncore_loop_timeout,
                map=self.map,
                use_poll=self.adj.asyncore_use_poll,
            )
        except (SystemExit, KeyboardInterrupt):
            self.close()

    def close(self):
        self.task_dispatcher.shutdown()
        wasyncore.close_all(self.map)


class BaseWSGIServer(wasyncore.dispatcher):
    channel_class = HTTPChannel
    next_channel_cleanup = 0
    socketmod = socket  # test shim
    asyncore = wasyncore  # test shim
    in_connection_overflow = False

    def __init__(
        self,
        application,
        map=None,
        _start=True,  # test shim
        _sock=None,  # test shim
        dispatcher=None,  # dispatcher
        adj=None,  # adjustments
        sockinfo=None,  # opaque object
        bind_socket=True,
        **kw
    ):
        if adj is None:
            adj = Adjustments(**kw)

        if adj.trusted_proxy or adj.clear_untrusted_proxy_headers:
            # wrap the application to deal with proxy headers
            # we wrap it here because webtest subclasses the TcpWSGIServer
            # directly and thus doesn't run any code that's in create_server
            application = proxy_headers_middleware(
                application,
                trusted_proxy=adj.trusted_proxy,
                trusted_proxy_count=adj.trusted_proxy_count,
                trusted_proxy_headers=adj.trusted_proxy_headers,
                clear_untrusted=adj.clear_untrusted_proxy_headers,
                log_untrusted=adj.log_untrusted_proxy_headers,
                logger=self.logger,
            )

        if map is None:
            # use a nonglobal socket map by default to hopefully prevent
            # conflicts with apps and libs that use the wasyncore global socket
            # map ala https://github.com/Pylons/waitress/issues/63
            map = {}
        if sockinfo is None:
            sockinfo = adj.listen[0]

        self.sockinfo = sockinfo
        self.family = sockinfo[0]
        self.socktype = sockinfo[1]
        self.application = application
        self.adj = adj
        self.trigger = trigger.trigger(map)
        if dispatcher is None:
            dispatcher = ThreadedTaskDispatcher()
            dispatcher.set_thread_count(self.adj.threads)

        self.task_dispatcher = dispatcher
        self.asyncore.dispatcher.__init__(self, _sock, map=map)
        if _sock is None:
            self.create_socket(self.family, self.socktype)
            if self.family == socket.AF_INET6:  # pragma: nocover
                self.socket.setsockopt(IPPROTO_IPV6, IPV6_V6ONLY, 1)

        self.set_reuse_addr()

        if bind_socket:
            self.bind_server_socket()

        self.effective_host, self.effective_port = self.getsockname()
        self.server_name = adj.server_name
        self.active_channels = {}
        if _start:
            self.accept_connections()

    def bind_server_socket(self):
        raise NotImplementedError  # pragma: no cover

    def getsockname(self):
        raise NotImplementedError  # pragma: no cover

    def accept_connections(self):
        self.accepting = True
        self.socket.listen(self.adj.backlog)  # Get around asyncore NT limit

    def add_task(self, task):
        self.task_dispatcher.add_task(task)

    def readable(self):
        now = time.time()
        if now >= self.next_channel_cleanup:
            self.next_channel_cleanup = now + self.adj.cleanup_interval
            self.maintenance(now)

        if self.accepting:
            if (
                not self.in_connection_overflow
                and len(self._map) >= self.adj.connection_limit
            ):
                self.in_connection_overflow = True
                self.logger.warning(
                    "total open connections reached the connection limit, "
                    "no longer accepting new connections"
                )
            elif (
                self.in_connection_overflow
                and len(self._map) < self.adj.connection_limit
            ):
                self.in_connection_overflow = False
                self.logger.info(
                    "total open connections dropped below the connection limit, "
                    "listening again"
                )
            return not self.in_connection_overflow
        return False

    def writable(self):
        return False

    def handle_read(self):
        pass

    def handle_connect(self):
        pass

    def handle_accept(self):
        try:
            v = self.accept()
            if v is None:
                return
            conn, addr = v
            self.set_socket_options(conn)
        except OSError:
            # Linux: On rare occasions we get a bogus socket back from
            # accept.  socketmodule.c:makesockaddr complains that the
            # address family is unknown.  We don't want the whole server
            # to shut down because of this.
            # macOS: On occasions when the remote has already closed the socket
            # before we got around to accepting it, when we try to set the
            # socket options it will fail. So instead just we log the error and
            # continue
            if self.adj.log_socket_errors:
                self.logger.warning("server accept() threw an exception", exc_info=True)
            return
        addr = self.fix_addr(addr)
        self.channel_class(self, conn, addr, self.adj, map=self._map)

    def run(self):
        try:
            self.asyncore.loop(
                timeout=self.adj.asyncore_loop_timeout,
                map=self._map,
                use_poll=self.adj.asyncore_use_poll,
            )
        except (SystemExit, KeyboardInterrupt):
            self.task_dispatcher.shutdown()

    def pull_trigger(self):
        self.trigger.pull_trigger()

    def set_socket_options(self, conn):
        pass

    def fix_addr(self, addr):
        return addr

    def maintenance(self, now):
        """
        Closes channels that have not had any activity in a while.

        The timeout is configured through adj.channel_timeout (seconds).
        """
        cutoff = now - self.adj.channel_timeout
        for channel in self.active_channels.values():
            if (not channel.requests) and channel.last_activity < cutoff:
                channel.will_close = True

    def print_listen(self, format_str):  # pragma: no cover
        self.log_info(format_str.format(self.effective_host, self.effective_port))

    def close(self):
        self.trigger.close()
        return wasyncore.dispatcher.close(self)


class TcpWSGIServer(BaseWSGIServer):
    def bind_server_socket(self):
        (_, _, _, sockaddr) = self.sockinfo
        self.bind(sockaddr)

    def getsockname(self):
        # Return the IP address, port as numeric
        return self.socketmod.getnameinfo(
            self.socket.getsockname(),
            self.socketmod.NI_NUMERICHOST | self.socketmod.NI_NUMERICSERV,
        )

    def set_socket_options(self, conn):
        for level, optname, value in self.adj.socket_options:
            conn.setsockopt(level, optname, value)


if hasattr(socket, "AF_UNIX"):

    class UnixWSGIServer(BaseWSGIServer):
        def __init__(
            self,
            application,
            map=None,
            _start=True,  # test shim
            _sock=None,  # test shim
            dispatcher=None,  # dispatcher
            adj=None,  # adjustments
            sockinfo=None,  # opaque object
            **kw
        ):
            if sockinfo is None:
                sockinfo = (socket.AF_UNIX, socket.SOCK_STREAM, None, None)

            super().__init__(
                application,
                map=map,
                _start=_start,
                _sock=_sock,
                dispatcher=dispatcher,
                adj=adj,
                sockinfo=sockinfo,
                **kw,
            )

        def bind_server_socket(self):
            cleanup_unix_socket(self.adj.unix_socket)
            self.bind(self.adj.unix_socket)
            if os.path.exists(self.adj.unix_socket):
                os.chmod(self.adj.unix_socket, self.adj.unix_socket_perms)

        def getsockname(self):
            return ("unix", self.socket.getsockname())

        def fix_addr(self, addr):
            return ("localhost", None)


# Compatibility alias.
WSGIServer = TcpWSGIServer

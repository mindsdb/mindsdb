##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
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
"""Adjustments are tunable parameters.
"""
import getopt
import socket
import warnings

from .compat import HAS_IPV6, WIN
from .proxy_headers import PROXY_HEADERS

truthy = frozenset(("t", "true", "y", "yes", "on", "1"))

KNOWN_PROXY_HEADERS = frozenset(
    header.lower().replace("_", "-") for header in PROXY_HEADERS
)


def asbool(s):
    """Return the boolean value ``True`` if the case-lowered value of string
    input ``s`` is any of ``t``, ``true``, ``y``, ``on``, or ``1``, otherwise
    return the boolean value ``False``.  If ``s`` is the value ``None``,
    return ``False``.  If ``s`` is already one of the boolean values ``True``
    or ``False``, return it."""
    if s is None:
        return False
    if isinstance(s, bool):
        return s
    s = str(s).strip()
    return s.lower() in truthy


def asoctal(s):
    """Convert the given octal string to an actual number."""
    return int(s, 8)


def aslist_cronly(value):
    if isinstance(value, str):
        value = filter(None, [x.strip() for x in value.splitlines()])
    return list(value)


def aslist(value):
    """Return a list of strings, separating the input based on newlines
    and, if flatten=True (the default), also split on spaces within
    each line."""
    values = aslist_cronly(value)
    result = []
    for value in values:
        subvalues = value.split()
        result.extend(subvalues)
    return result


def asset(value):
    return set(aslist(value))


def slash_fixed_str(s):
    s = s.strip()
    if s:
        # always have a leading slash, replace any number of leading slashes
        # with a single slash, and strip any trailing slashes
        s = "/" + s.lstrip("/").rstrip("/")
    return s


def str_iftruthy(s):
    return str(s) if s else None


def as_socket_list(sockets):
    """Checks if the elements in the list are of type socket and
    removes them if not."""
    return [sock for sock in sockets if isinstance(sock, socket.socket)]


class _str_marker(str):
    pass


class _int_marker(int):
    pass


class Adjustments:
    """This class contains tunable parameters."""

    _params = (
        ("host", str),
        ("port", int),
        ("ipv4", asbool),
        ("ipv6", asbool),
        ("listen", aslist),
        ("threads", int),
        ("trusted_proxy", str_iftruthy),
        ("trusted_proxy_count", int),
        ("trusted_proxy_headers", asset),
        ("log_untrusted_proxy_headers", asbool),
        ("clear_untrusted_proxy_headers", asbool),
        ("url_scheme", str),
        ("url_prefix", slash_fixed_str),
        ("backlog", int),
        ("recv_bytes", int),
        ("send_bytes", int),
        ("outbuf_overflow", int),
        ("outbuf_high_watermark", int),
        ("inbuf_overflow", int),
        ("connection_limit", int),
        ("cleanup_interval", int),
        ("channel_timeout", int),
        ("log_socket_errors", asbool),
        ("max_request_header_size", int),
        ("max_request_body_size", int),
        ("expose_tracebacks", asbool),
        ("ident", str_iftruthy),
        ("asyncore_loop_timeout", int),
        ("asyncore_use_poll", asbool),
        ("unix_socket", str),
        ("unix_socket_perms", asoctal),
        ("sockets", as_socket_list),
        ("channel_request_lookahead", int),
        ("server_name", str),
    )

    _param_map = dict(_params)

    # hostname or IP address to listen on
    host = _str_marker("0.0.0.0")

    # TCP port to listen on
    port = _int_marker(8080)

    listen = [f"{host}:{port}"]

    # number of threads available for tasks
    threads = 4

    # Host allowed to overrid ``wsgi.url_scheme`` via header
    trusted_proxy = None

    # How many proxies we trust when chained
    #
    # X-Forwarded-For: 192.0.2.1, "[2001:db8::1]"
    #
    # or
    #
    # Forwarded: for=192.0.2.1, For="[2001:db8::1]"
    #
    # means there were (potentially), two proxies involved. If we know there is
    # only 1 valid proxy, then that initial IP address "192.0.2.1" is not
    # trusted and we completely ignore it. If there are two trusted proxies in
    # the path, this value should be set to a higher number.
    trusted_proxy_count = None

    # Which of the proxy headers should we trust, this is a set where you
    # either specify forwarded or one or more of forwarded-host, forwarded-for,
    # forwarded-proto, forwarded-port.
    trusted_proxy_headers = set()

    # Would you like waitress to log warnings about untrusted proxy headers
    # that were encountered while processing the proxy headers? This only makes
    # sense to set when you have a trusted_proxy, and you expect the upstream
    # proxy server to filter invalid headers
    log_untrusted_proxy_headers = False

    # Changed this parameter to True by default in 3.x
    clear_untrusted_proxy_headers = True

    # default ``wsgi.url_scheme`` value
    url_scheme = "http"

    # default ``SCRIPT_NAME`` value, also helps reset ``PATH_INFO``
    # when nonempty
    url_prefix = ""

    # server identity (sent in Server: header)
    ident = "waitress"

    # backlog is the value waitress passes to pass to socket.listen() This is
    # the maximum number of incoming TCP connections that will wait in an OS
    # queue for an available channel.  From listen(1): "If a connection
    # request arrives when the queue is full, the client may receive an error
    # with an indication of ECONNREFUSED or, if the underlying protocol
    # supports retransmission, the request may be ignored so that a later
    # reattempt at connection succeeds."
    backlog = 1024

    # recv_bytes is the argument to pass to socket.recv().
    recv_bytes = 8192

    # deprecated setting controls how many bytes will be buffered before
    # being flushed to the socket
    send_bytes = 1

    # A tempfile should be created if the pending output is larger than
    # outbuf_overflow, which is measured in bytes. The default is 1MB.  This
    # is conservative.
    outbuf_overflow = 1048576

    # The app_iter will pause when pending output is larger than this value
    # in bytes.
    outbuf_high_watermark = 16777216

    # A tempfile should be created if the pending input is larger than
    # inbuf_overflow, which is measured in bytes. The default is 512K.  This
    # is conservative.
    inbuf_overflow = 524288

    # Stop creating new channels if too many are already active (integer).
    # Each channel consumes at least one file descriptor, and, depending on
    # the input and output body sizes, potentially up to three.  The default
    # is conservative, but you may need to increase the number of file
    # descriptors available to the Waitress process on most platforms in
    # order to safely change it (see ``ulimit -a`` "open files" setting).
    # Note that this doesn't control the maximum number of TCP connections
    # that can be waiting for processing; the ``backlog`` argument controls
    # that.
    connection_limit = 100

    # Minimum seconds between cleaning up inactive channels.
    cleanup_interval = 30

    # Maximum seconds to leave an inactive connection open.
    channel_timeout = 120

    # Boolean: turn off to not log premature client disconnects.
    log_socket_errors = True

    # maximum number of bytes of all request headers combined (256K default)
    max_request_header_size = 262144

    # maximum number of bytes in request body (1GB default)
    max_request_body_size = 1073741824

    # expose tracebacks of uncaught exceptions
    expose_tracebacks = False

    # Path to a Unix domain socket to use.
    unix_socket = None

    # Path to a Unix domain socket to use.
    unix_socket_perms = 0o600

    # The socket options to set on receiving a connection.  It is a list of
    # (level, optname, value) tuples.  TCP_NODELAY disables the Nagle
    # algorithm for writes (Waitress already buffers its writes).
    socket_options = [
        (socket.SOL_TCP, socket.TCP_NODELAY, 1),
    ]

    # The asyncore.loop timeout value
    asyncore_loop_timeout = 1

    # The asyncore.loop flag to use poll() instead of the default select().
    asyncore_use_poll = False

    # Enable IPv4 by default
    ipv4 = True

    # Enable IPv6 by default
    ipv6 = True

    # A list of sockets that waitress will use to accept connections. They can
    # be used for e.g. socket activation
    sockets = []

    # By setting this to a value larger than zero, each channel stays readable
    # and continues to read requests from the client even if a request is still
    # running, until the number of buffered requests exceeds this value.
    # This allows detecting if a client closed the connection while its request
    # is being processed.
    channel_request_lookahead = 0

    # This setting controls the SERVER_NAME of the WSGI environment, this is
    # only ever used if the remote client sent a request without a Host header
    # (or when using the Proxy settings, without forwarding a Host header)
    server_name = "waitress.invalid"

    def __init__(self, **kw):
        if "listen" in kw and ("host" in kw or "port" in kw):
            raise ValueError("host or port may not be set if listen is set.")

        if "listen" in kw and "sockets" in kw:
            raise ValueError("socket may not be set if listen is set.")

        if "sockets" in kw and ("host" in kw or "port" in kw):
            raise ValueError("host or port may not be set if sockets is set.")

        if "sockets" in kw and "unix_socket" in kw:
            raise ValueError("unix_socket may not be set if sockets is set")

        if "unix_socket" in kw and ("host" in kw or "port" in kw):
            raise ValueError("unix_socket may not be set if host or port is set")

        if "unix_socket" in kw and "listen" in kw:
            raise ValueError("unix_socket may not be set if listen is set")

        if "send_bytes" in kw:
            warnings.warn(
                "send_bytes will be removed in a future release", DeprecationWarning
            )

        for k, v in kw.items():
            if k not in self._param_map:
                raise ValueError("Unknown adjustment %r" % k)
            setattr(self, k, self._param_map[k](v))

        if not isinstance(self.host, _str_marker) or not isinstance(
            self.port, _int_marker
        ):
            self.listen = [f"{self.host}:{self.port}"]

        enabled_families = socket.AF_UNSPEC

        if not self.ipv4 and not HAS_IPV6:  # pragma: no cover
            raise ValueError(
                "IPv4 is disabled but IPv6 is not available. Cowardly refusing to start."
            )

        if self.ipv4 and not self.ipv6:
            enabled_families = socket.AF_INET

        if not self.ipv4 and self.ipv6 and HAS_IPV6:
            enabled_families = socket.AF_INET6

        wanted_sockets = []
        hp_pairs = []
        for i in self.listen:
            if ":" in i:
                (host, port) = i.rsplit(":", 1)

                # IPv6 we need to make sure that we didn't split on the address
                if "]" in port:  # pragma: nocover
                    (host, port) = (i, str(self.port))
            else:
                (host, port) = (i, str(self.port))

            if WIN:  # pragma: no cover
                try:
                    # Try turning the port into an integer
                    port = int(port)

                except Exception:
                    raise ValueError(
                        "Windows does not support service names instead of port numbers"
                    )

            try:
                if "[" in host and "]" in host:  # pragma: nocover
                    host = host.strip("[").rstrip("]")

                if host == "*":
                    host = None

                for s in socket.getaddrinfo(
                    host,
                    port,
                    enabled_families,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    socket.AI_PASSIVE,
                ):
                    (family, socktype, proto, _, sockaddr) = s

                    # It seems that getaddrinfo() may sometimes happily return
                    # the same result multiple times, this of course makes
                    # bind() very unhappy...
                    #
                    # Split on %, and drop the zone-index from the host in the
                    # sockaddr. Works around a bug in OS X whereby
                    # getaddrinfo() returns the same link-local interface with
                    # two different zone-indices (which makes no sense what so
                    # ever...) yet treats them equally when we attempt to bind().
                    if (
                        sockaddr[1] == 0
                        or (sockaddr[0].split("%", 1)[0], sockaddr[1]) not in hp_pairs
                    ):
                        wanted_sockets.append((family, socktype, proto, sockaddr))
                        hp_pairs.append((sockaddr[0].split("%", 1)[0], sockaddr[1]))

            except Exception:
                raise ValueError("Invalid host/port specified.")

        if self.trusted_proxy_count is not None and self.trusted_proxy is None:
            raise ValueError(
                "trusted_proxy_count has no meaning without setting " "trusted_proxy"
            )

        elif self.trusted_proxy_count is None:
            self.trusted_proxy_count = 1

        if self.trusted_proxy_headers and self.trusted_proxy is None:
            raise ValueError(
                "trusted_proxy_headers has no meaning without setting " "trusted_proxy"
            )

        if self.trusted_proxy_headers:
            self.trusted_proxy_headers = {
                header.lower() for header in self.trusted_proxy_headers
            }

            unknown_values = self.trusted_proxy_headers - KNOWN_PROXY_HEADERS
            if unknown_values:
                raise ValueError(
                    "Received unknown trusted_proxy_headers value (%s) expected one "
                    "of %s"
                    % (", ".join(unknown_values), ", ".join(KNOWN_PROXY_HEADERS))
                )

            if (
                "forwarded" in self.trusted_proxy_headers
                and self.trusted_proxy_headers - {"forwarded"}
            ):
                raise ValueError(
                    "The Forwarded proxy header and the "
                    "X-Forwarded-{By,Host,Proto,Port,For} headers are mutually "
                    "exclusive. Can't trust both!"
                )

        elif self.trusted_proxy is not None:
            warnings.warn(
                "No proxy headers were marked as trusted, but trusted_proxy was set. "
                "Implicitly trusting X-Forwarded-Proto for backwards compatibility. "
                "This will be removed in future versions of waitress.",
                DeprecationWarning,
            )
            self.trusted_proxy_headers = {"x-forwarded-proto"}

        self.listen = wanted_sockets

        self.check_sockets(self.sockets)

    @classmethod
    def parse_args(cls, argv):
        """Pre-parse command line arguments for input into __init__.  Note that
        this does not cast values into adjustment types, it just creates a
        dictionary suitable for passing into __init__, where __init__ does the
        casting.
        """
        long_opts = ["help", "call"]
        for opt, cast in cls._params:
            opt = opt.replace("_", "-")
            if cast is asbool:
                long_opts.append(opt)
                long_opts.append("no-" + opt)
            else:
                long_opts.append(opt + "=")

        kw = {
            "help": False,
            "call": False,
        }

        opts, args = getopt.getopt(argv, "", long_opts)
        for opt, value in opts:
            param = opt.lstrip("-").replace("-", "_")

            if param == "listen":
                kw["listen"] = "{} {}".format(kw.get("listen", ""), value)
                continue

            if param.startswith("no_"):
                param = param[3:]
                kw[param] = "false"
            elif param in ("help", "call"):
                kw[param] = True
            elif cls._param_map[param] is asbool:
                kw[param] = "true"
            else:
                kw[param] = value

        return kw, args

    @classmethod
    def check_sockets(cls, sockets):
        has_unix_socket = False
        has_inet_socket = False
        has_unsupported_socket = False
        for sock in sockets:
            if (
                sock.family == socket.AF_INET or sock.family == socket.AF_INET6
            ) and sock.type == socket.SOCK_STREAM:
                has_inet_socket = True
            elif (
                hasattr(socket, "AF_UNIX")
                and sock.family == socket.AF_UNIX
                and sock.type == socket.SOCK_STREAM
            ):
                has_unix_socket = True
            else:
                has_unsupported_socket = True
        if has_unix_socket and has_inet_socket:
            raise ValueError("Internet and UNIX sockets may not be mixed.")
        if has_unsupported_socket:
            raise ValueError("Only Internet or UNIX stream sockets may be used.")

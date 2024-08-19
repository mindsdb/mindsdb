# Copyright 2011-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import annotations

import collections
import contextlib
import copy
import logging
import os
import platform
import socket
import ssl
import sys
import threading
import time
import weakref
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Union,
)

import bson
from bson import DEFAULT_CODEC_OPTIONS
from pymongo import __version__, _csot, helpers
from pymongo.client_session import _validate_session_write_concern
from pymongo.common import (
    MAX_BSON_SIZE,
    MAX_CONNECTING,
    MAX_IDLE_TIME_SEC,
    MAX_MESSAGE_SIZE,
    MAX_POOL_SIZE,
    MAX_WIRE_VERSION,
    MAX_WRITE_BATCH_SIZE,
    MIN_POOL_SIZE,
    ORDERED_TYPES,
    WAIT_QUEUE_TIMEOUT,
)
from pymongo.errors import (  # type:ignore[attr-defined]
    AutoReconnect,
    ConfigurationError,
    ConnectionFailure,
    DocumentTooLarge,
    ExecutionTimeout,
    InvalidOperation,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
    WaitQueueTimeoutError,
    _CertificateError,
)
from pymongo.hello import Hello, HelloCompat
from pymongo.helpers import _handle_reauth
from pymongo.lock import _create_lock
from pymongo.logger import (
    _CONNECTION_LOGGER,
    _ConnectionStatusMessage,
    _debug_log,
    _verbose_connection_error_reason,
)
from pymongo.monitoring import (
    ConnectionCheckOutFailedReason,
    ConnectionClosedReason,
    _EventListeners,
)
from pymongo.network import command, receive_message
from pymongo.read_preferences import ReadPreference
from pymongo.server_api import _add_to_command
from pymongo.server_type import SERVER_TYPE
from pymongo.socket_checker import SocketChecker
from pymongo.ssl_support import HAS_SNI, SSLError

if TYPE_CHECKING:
    from bson import CodecOptions
    from bson.objectid import ObjectId
    from pymongo.auth import MongoCredential, _AuthContext
    from pymongo.client_session import ClientSession
    from pymongo.compression_support import (
        CompressionSettings,
        SnappyContext,
        ZlibContext,
        ZstdContext,
    )
    from pymongo.driver_info import DriverInfo
    from pymongo.message import _OpMsg, _OpReply
    from pymongo.mongo_client import MongoClient, _MongoClientErrorHandler
    from pymongo.pyopenssl_context import SSLContext, _sslConn
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import _ServerMode
    from pymongo.server_api import ServerApi
    from pymongo.typings import ClusterTime, _Address, _CollationIn
    from pymongo.write_concern import WriteConcern

try:
    from fcntl import F_GETFD, F_SETFD, FD_CLOEXEC, fcntl

    def _set_non_inheritable_non_atomic(fd: int) -> None:
        """Set the close-on-exec flag on the given file descriptor."""
        flags = fcntl(fd, F_GETFD)
        fcntl(fd, F_SETFD, flags | FD_CLOEXEC)

except ImportError:
    # Windows, various platforms we don't claim to support
    # (Jython, IronPython, ..), systems that don't provide
    # everything we need from fcntl, etc.
    def _set_non_inheritable_non_atomic(fd: int) -> None:  # noqa: ARG001
        """Dummy function for platforms that don't provide fcntl."""


_MAX_TCP_KEEPIDLE = 120
_MAX_TCP_KEEPINTVL = 10
_MAX_TCP_KEEPCNT = 9

if sys.platform == "win32":
    try:
        import _winreg as winreg
    except ImportError:
        import winreg

    def _query(key, name, default):
        try:
            value, _ = winreg.QueryValueEx(key, name)
            # Ensure the value is a number or raise ValueError.
            return int(value)
        except (OSError, ValueError):
            # QueryValueEx raises OSError when the key does not exist (i.e.
            # the system is using the Windows default value).
            return default

    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Services\Tcpip\Parameters"
        ) as key:
            _WINDOWS_TCP_IDLE_MS = _query(key, "KeepAliveTime", 7200000)
            _WINDOWS_TCP_INTERVAL_MS = _query(key, "KeepAliveInterval", 1000)
    except OSError:
        # We could not check the default values because winreg.OpenKey failed.
        # Assume the system is using the default values.
        _WINDOWS_TCP_IDLE_MS = 7200000
        _WINDOWS_TCP_INTERVAL_MS = 1000

    def _set_keepalive_times(sock):
        idle_ms = min(_WINDOWS_TCP_IDLE_MS, _MAX_TCP_KEEPIDLE * 1000)
        interval_ms = min(_WINDOWS_TCP_INTERVAL_MS, _MAX_TCP_KEEPINTVL * 1000)
        if idle_ms < _WINDOWS_TCP_IDLE_MS or interval_ms < _WINDOWS_TCP_INTERVAL_MS:
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, idle_ms, interval_ms))

else:

    def _set_tcp_option(sock: socket.socket, tcp_option: str, max_value: int) -> None:
        if hasattr(socket, tcp_option):
            sockopt = getattr(socket, tcp_option)
            try:
                # PYTHON-1350 - NetBSD doesn't implement getsockopt for
                # TCP_KEEPIDLE and friends. Don't attempt to set the
                # values there.
                default = sock.getsockopt(socket.IPPROTO_TCP, sockopt)
                if default > max_value:
                    sock.setsockopt(socket.IPPROTO_TCP, sockopt, max_value)
            except OSError:
                pass

    def _set_keepalive_times(sock: socket.socket) -> None:
        _set_tcp_option(sock, "TCP_KEEPIDLE", _MAX_TCP_KEEPIDLE)
        _set_tcp_option(sock, "TCP_KEEPINTVL", _MAX_TCP_KEEPINTVL)
        _set_tcp_option(sock, "TCP_KEEPCNT", _MAX_TCP_KEEPCNT)


_METADATA: dict[str, Any] = {"driver": {"name": "PyMongo", "version": __version__}}

if sys.platform.startswith("linux"):
    # platform.linux_distribution was deprecated in Python 3.5
    # and removed in Python 3.8. Starting in Python 3.5 it
    # raises DeprecationWarning
    # DeprecationWarning: dist() and linux_distribution() functions are deprecated in Python 3.5
    _name = platform.system()
    _METADATA["os"] = {
        "type": _name,
        "name": _name,
        "architecture": platform.machine(),
        # Kernel version (e.g. 4.4.0-17-generic).
        "version": platform.release(),
    }
elif sys.platform == "darwin":
    _METADATA["os"] = {
        "type": platform.system(),
        "name": platform.system(),
        "architecture": platform.machine(),
        # (mac|i|tv)OS(X) version (e.g. 10.11.6) instead of darwin
        # kernel version.
        "version": platform.mac_ver()[0],
    }
elif sys.platform == "win32":
    _ver = sys.getwindowsversion()
    _METADATA["os"] = {
        "type": "Windows",
        "name": "Windows",
        # Avoid using platform calls, see PYTHON-4455.
        "architecture": os.environ.get("PROCESSOR_ARCHITECTURE") or platform.machine(),
        # Windows patch level (e.g. 10.0.17763-SP0).
        "version": ".".join(map(str, _ver[:3])) + f"-SP{_ver[-1] or '0'}",
    }
elif sys.platform.startswith("java"):
    _name, _ver, _arch = platform.java_ver()[-1]
    _METADATA["os"] = {
        # Linux, Windows 7, Mac OS X, etc.
        "type": _name,
        "name": _name,
        # x86, x86_64, AMD64, etc.
        "architecture": _arch,
        # Linux kernel version, OSX version, etc.
        "version": _ver,
    }
else:
    # Get potential alias (e.g. SunOS 5.11 becomes Solaris 2.11)
    _aliased = platform.system_alias(platform.system(), platform.release(), platform.version())
    _METADATA["os"] = {
        "type": platform.system(),
        "name": " ".join([part for part in _aliased[:2] if part]),
        "architecture": platform.machine(),
        "version": _aliased[2],
    }

if platform.python_implementation().startswith("PyPy"):
    _METADATA["platform"] = " ".join(
        (
            platform.python_implementation(),
            ".".join(map(str, sys.pypy_version_info)),  # type: ignore
            "(Python %s)" % ".".join(map(str, sys.version_info)),
        )
    )
elif sys.platform.startswith("java"):
    _METADATA["platform"] = " ".join(
        (
            platform.python_implementation(),
            ".".join(map(str, sys.version_info)),
            "(%s)" % " ".join((platform.system(), platform.release())),
        )
    )
else:
    _METADATA["platform"] = " ".join(
        (platform.python_implementation(), ".".join(map(str, sys.version_info)))
    )

DOCKER_ENV_PATH = "/.dockerenv"
ENV_VAR_K8S = "KUBERNETES_SERVICE_HOST"

RUNTIME_NAME_DOCKER = "docker"
ORCHESTRATOR_NAME_K8S = "kubernetes"


def get_container_env_info() -> dict[str, str]:
    """Returns the runtime and orchestrator of a container.
    If neither value is present, the metadata client.env.container field will be omitted."""
    container = {}

    if Path(DOCKER_ENV_PATH).exists():
        container["runtime"] = RUNTIME_NAME_DOCKER
    if os.getenv(ENV_VAR_K8S):
        container["orchestrator"] = ORCHESTRATOR_NAME_K8S

    return container


def _is_lambda() -> bool:
    if os.getenv("AWS_LAMBDA_RUNTIME_API"):
        return True
    env = os.getenv("AWS_EXECUTION_ENV")
    if env:
        return env.startswith("AWS_Lambda_")
    return False


def _is_azure_func() -> bool:
    return bool(os.getenv("FUNCTIONS_WORKER_RUNTIME"))


def _is_gcp_func() -> bool:
    return bool(os.getenv("K_SERVICE") or os.getenv("FUNCTION_NAME"))


def _is_vercel() -> bool:
    return bool(os.getenv("VERCEL"))


def _is_faas() -> bool:
    return _is_lambda() or _is_azure_func() or _is_gcp_func() or _is_vercel()


def _getenv_int(key: str) -> Optional[int]:
    """Like os.getenv but returns an int, or None if the value is missing/malformed."""
    val = os.getenv(key)
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        return None


def _metadata_env() -> dict[str, Any]:
    env: dict[str, Any] = {}
    container = get_container_env_info()
    if container:
        env["container"] = container
    # Skip if multiple (or no) envs are matched.
    if (_is_lambda(), _is_azure_func(), _is_gcp_func(), _is_vercel()).count(True) != 1:
        return env
    if _is_lambda():
        env["name"] = "aws.lambda"
        region = os.getenv("AWS_REGION")
        if region:
            env["region"] = region
        memory_mb = _getenv_int("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
        if memory_mb is not None:
            env["memory_mb"] = memory_mb
    elif _is_azure_func():
        env["name"] = "azure.func"
    elif _is_gcp_func():
        env["name"] = "gcp.func"
        region = os.getenv("FUNCTION_REGION")
        if region:
            env["region"] = region
        memory_mb = _getenv_int("FUNCTION_MEMORY_MB")
        if memory_mb is not None:
            env["memory_mb"] = memory_mb
        timeout_sec = _getenv_int("FUNCTION_TIMEOUT_SEC")
        if timeout_sec is not None:
            env["timeout_sec"] = timeout_sec
    elif _is_vercel():
        env["name"] = "vercel"
        region = os.getenv("VERCEL_REGION")
        if region:
            env["region"] = region
    return env


_MAX_METADATA_SIZE = 512


# See: https://github.com/mongodb/specifications/blob/5112bcc/source/mongodb-handshake/handshake.rst#limitations
def _truncate_metadata(metadata: MutableMapping[str, Any]) -> None:
    """Perform metadata truncation."""
    if len(bson.encode(metadata)) <= _MAX_METADATA_SIZE:
        return
    # 1. Omit fields from env except env.name.
    env_name = metadata.get("env", {}).get("name")
    if env_name:
        metadata["env"] = {"name": env_name}
    if len(bson.encode(metadata)) <= _MAX_METADATA_SIZE:
        return
    # 2. Omit fields from os except os.type.
    os_type = metadata.get("os", {}).get("type")
    if os_type:
        metadata["os"] = {"type": os_type}
    if len(bson.encode(metadata)) <= _MAX_METADATA_SIZE:
        return
    # 3. Omit the env document entirely.
    metadata.pop("env", None)
    encoded_size = len(bson.encode(metadata))
    if encoded_size <= _MAX_METADATA_SIZE:
        return
    # 4. Truncate platform.
    overflow = encoded_size - _MAX_METADATA_SIZE
    plat = metadata.get("platform", "")
    if plat:
        plat = plat[:-overflow]
    if plat:
        metadata["platform"] = plat
    else:
        metadata.pop("platform", None)


# If the first getaddrinfo call of this interpreter's life is on a thread,
# while the main thread holds the import lock, getaddrinfo deadlocks trying
# to import the IDNA codec. Import it here, where presumably we're on the
# main thread, to avoid the deadlock. See PYTHON-607.
"foo".encode("idna")


def _raise_connection_failure(
    address: Any,
    error: Exception,
    msg_prefix: Optional[str] = None,
    timeout_details: Optional[dict[str, float]] = None,
) -> NoReturn:
    """Convert a socket.error to ConnectionFailure and raise it."""
    host, port = address
    # If connecting to a Unix socket, port will be None.
    if port is not None:
        msg = "%s:%d: %s" % (host, port, error)
    else:
        msg = f"{host}: {error}"
    if msg_prefix:
        msg = msg_prefix + msg
    if "configured timeouts" not in msg:
        msg += format_timeout_details(timeout_details)
    if isinstance(error, socket.timeout):
        raise NetworkTimeout(msg) from error
    elif isinstance(error, SSLError) and "timed out" in str(error):
        # Eventlet does not distinguish TLS network timeouts from other
        # SSLErrors (https://github.com/eventlet/eventlet/issues/692).
        # Luckily, we can work around this limitation because the phrase
        # 'timed out' appears in all the timeout related SSLErrors raised.
        raise NetworkTimeout(msg) from error
    else:
        raise AutoReconnect(msg) from error


def _cond_wait(condition: threading.Condition, deadline: Optional[float]) -> bool:
    timeout = deadline - time.monotonic() if deadline else None
    return condition.wait(timeout)


def _get_timeout_details(options: PoolOptions) -> dict[str, float]:
    details = {}
    timeout = _csot.get_timeout()
    socket_timeout = options.socket_timeout
    connect_timeout = options.connect_timeout
    if timeout:
        details["timeoutMS"] = timeout * 1000
    if socket_timeout and not timeout:
        details["socketTimeoutMS"] = socket_timeout * 1000
    if connect_timeout:
        details["connectTimeoutMS"] = connect_timeout * 1000
    return details


def format_timeout_details(details: Optional[dict[str, float]]) -> str:
    result = ""
    if details:
        result += " (configured timeouts:"
        for timeout in ["socketTimeoutMS", "timeoutMS", "connectTimeoutMS"]:
            if timeout in details:
                result += f" {timeout}: {details[timeout]}ms,"
        result = result[:-1]
        result += ")"
    return result


class PoolOptions:
    """Read only connection pool options for a MongoClient.

    Should not be instantiated directly by application developers. Access
    a client's pool options via
    :attr:`~pymongo.client_options.ClientOptions.pool_options` instead::

      pool_opts = client.options.pool_options
      pool_opts.max_pool_size
      pool_opts.min_pool_size

    """

    __slots__ = (
        "__max_pool_size",
        "__min_pool_size",
        "__max_idle_time_seconds",
        "__connect_timeout",
        "__socket_timeout",
        "__wait_queue_timeout",
        "__ssl_context",
        "__tls_allow_invalid_hostnames",
        "__event_listeners",
        "__appname",
        "__driver",
        "__metadata",
        "__compression_settings",
        "__max_connecting",
        "__pause_enabled",
        "__server_api",
        "__load_balanced",
        "__credentials",
    )

    def __init__(
        self,
        max_pool_size: int = MAX_POOL_SIZE,
        min_pool_size: int = MIN_POOL_SIZE,
        max_idle_time_seconds: Optional[int] = MAX_IDLE_TIME_SEC,
        connect_timeout: Optional[float] = None,
        socket_timeout: Optional[float] = None,
        wait_queue_timeout: Optional[int] = WAIT_QUEUE_TIMEOUT,
        ssl_context: Optional[SSLContext] = None,
        tls_allow_invalid_hostnames: bool = False,
        event_listeners: Optional[_EventListeners] = None,
        appname: Optional[str] = None,
        driver: Optional[DriverInfo] = None,
        compression_settings: Optional[CompressionSettings] = None,
        max_connecting: int = MAX_CONNECTING,
        pause_enabled: bool = True,
        server_api: Optional[ServerApi] = None,
        load_balanced: Optional[bool] = None,
        credentials: Optional[MongoCredential] = None,
    ):
        self.__max_pool_size = max_pool_size
        self.__min_pool_size = min_pool_size
        self.__max_idle_time_seconds = max_idle_time_seconds
        self.__connect_timeout = connect_timeout
        self.__socket_timeout = socket_timeout
        self.__wait_queue_timeout = wait_queue_timeout
        self.__ssl_context = ssl_context
        self.__tls_allow_invalid_hostnames = tls_allow_invalid_hostnames
        self.__event_listeners = event_listeners
        self.__appname = appname
        self.__driver = driver
        self.__compression_settings = compression_settings
        self.__max_connecting = max_connecting
        self.__pause_enabled = pause_enabled
        self.__server_api = server_api
        self.__load_balanced = load_balanced
        self.__credentials = credentials
        self.__metadata = copy.deepcopy(_METADATA)
        if appname:
            self.__metadata["application"] = {"name": appname}

        # Combine the "driver" MongoClient option with PyMongo's info, like:
        # {
        #    'driver': {
        #        'name': 'PyMongo|MyDriver',
        #        'version': '4.2.0|1.2.3',
        #    },
        #    'platform': 'CPython 3.8.0|MyPlatform'
        # }
        if driver:
            if driver.name:
                self.__metadata["driver"]["name"] = "{}|{}".format(
                    _METADATA["driver"]["name"],
                    driver.name,
                )
            if driver.version:
                self.__metadata["driver"]["version"] = "{}|{}".format(
                    _METADATA["driver"]["version"],
                    driver.version,
                )
            if driver.platform:
                self.__metadata["platform"] = "{}|{}".format(_METADATA["platform"], driver.platform)

        env = _metadata_env()
        if env:
            self.__metadata["env"] = env

        _truncate_metadata(self.__metadata)

    @property
    def _credentials(self) -> Optional[MongoCredential]:
        """A :class:`~pymongo.auth.MongoCredentials` instance or None."""
        return self.__credentials

    @property
    def non_default_options(self) -> dict[str, Any]:
        """The non-default options this pool was created with.

        Added for CMAP's :class:`PoolCreatedEvent`.
        """
        opts = {}
        if self.__max_pool_size != MAX_POOL_SIZE:
            opts["maxPoolSize"] = self.__max_pool_size
        if self.__min_pool_size != MIN_POOL_SIZE:
            opts["minPoolSize"] = self.__min_pool_size
        if self.__max_idle_time_seconds != MAX_IDLE_TIME_SEC:
            assert self.__max_idle_time_seconds is not None
            opts["maxIdleTimeMS"] = self.__max_idle_time_seconds * 1000
        if self.__wait_queue_timeout != WAIT_QUEUE_TIMEOUT:
            assert self.__wait_queue_timeout is not None
            opts["waitQueueTimeoutMS"] = self.__wait_queue_timeout * 1000
        if self.__max_connecting != MAX_CONNECTING:
            opts["maxConnecting"] = self.__max_connecting
        return opts

    @property
    def max_pool_size(self) -> float:
        """The maximum allowable number of concurrent connections to each
        connected server. Requests to a server will block if there are
        `maxPoolSize` outstanding connections to the requested server.
        Defaults to 100. Cannot be 0.

        When a server's pool has reached `max_pool_size`, operations for that
        server block waiting for a socket to be returned to the pool. If
        ``waitQueueTimeoutMS`` is set, a blocked operation will raise
        :exc:`~pymongo.errors.ConnectionFailure` after a timeout.
        By default ``waitQueueTimeoutMS`` is not set.
        """
        return self.__max_pool_size

    @property
    def min_pool_size(self) -> int:
        """The minimum required number of concurrent connections that the pool
        will maintain to each connected server. Default is 0.
        """
        return self.__min_pool_size

    @property
    def max_connecting(self) -> int:
        """The maximum number of concurrent connection creation attempts per
        pool. Defaults to 2.
        """
        return self.__max_connecting

    @property
    def pause_enabled(self) -> bool:
        return self.__pause_enabled

    @property
    def max_idle_time_seconds(self) -> Optional[int]:
        """The maximum number of seconds that a connection can remain
        idle in the pool before being removed and replaced. Defaults to
        `None` (no limit).
        """
        return self.__max_idle_time_seconds

    @property
    def connect_timeout(self) -> Optional[float]:
        """How long a connection can take to be opened before timing out."""
        return self.__connect_timeout

    @property
    def socket_timeout(self) -> Optional[float]:
        """How long a send or receive on a socket can take before timing out."""
        return self.__socket_timeout

    @property
    def wait_queue_timeout(self) -> Optional[int]:
        """How long a thread will wait for a socket from the pool if the pool
        has no free sockets.
        """
        return self.__wait_queue_timeout

    @property
    def _ssl_context(self) -> Optional[SSLContext]:
        """An SSLContext instance or None."""
        return self.__ssl_context

    @property
    def tls_allow_invalid_hostnames(self) -> bool:
        """If True skip ssl.match_hostname."""
        return self.__tls_allow_invalid_hostnames

    @property
    def _event_listeners(self) -> Optional[_EventListeners]:
        """An instance of pymongo.monitoring._EventListeners."""
        return self.__event_listeners

    @property
    def appname(self) -> Optional[str]:
        """The application name, for sending with hello in server handshake."""
        return self.__appname

    @property
    def driver(self) -> Optional[DriverInfo]:
        """Driver name and version, for sending with hello in handshake."""
        return self.__driver

    @property
    def _compression_settings(self) -> Optional[CompressionSettings]:
        return self.__compression_settings

    @property
    def metadata(self) -> dict[str, Any]:
        """A dict of metadata about the application, driver, os, and platform."""
        return self.__metadata.copy()

    @property
    def server_api(self) -> Optional[ServerApi]:
        """A pymongo.server_api.ServerApi or None."""
        return self.__server_api

    @property
    def load_balanced(self) -> Optional[bool]:
        """True if this Pool is configured in load balanced mode."""
        return self.__load_balanced


class _CancellationContext:
    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        """Cancel this context."""
        self._cancelled = True

    @property
    def cancelled(self) -> bool:
        """Was cancel called?"""
        return self._cancelled


class Connection:
    """Store a connection with some metadata.

    :param conn: a raw connection object
    :param pool: a Pool instance
    :param address: the server's (host, port)
    :param id: the id of this socket in it's pool
    """

    def __init__(
        self, conn: Union[socket.socket, _sslConn], pool: Pool, address: tuple[str, int], id: int
    ):
        self.pool_ref = weakref.ref(pool)
        self.conn = conn
        self.address = address
        self.id = id
        self.closed = False
        self.last_checkin_time = time.monotonic()
        self.performed_handshake = False
        self.is_writable: bool = False
        self.max_wire_version = MAX_WIRE_VERSION
        self.max_bson_size = MAX_BSON_SIZE
        self.max_message_size = MAX_MESSAGE_SIZE
        self.max_write_batch_size = MAX_WRITE_BATCH_SIZE
        self.supports_sessions = False
        self.hello_ok: bool = False
        self.is_mongos = False
        self.op_msg_enabled = False
        self.listeners = pool.opts._event_listeners
        self.enabled_for_cmap = pool.enabled_for_cmap
        self.enabled_for_logging = pool.enabled_for_logging
        self.compression_settings = pool.opts._compression_settings
        self.compression_context: Union[SnappyContext, ZlibContext, ZstdContext, None] = None
        self.socket_checker: SocketChecker = SocketChecker()
        self.oidc_token_gen_id: Optional[int] = None
        # Support for mechanism negotiation on the initial handshake.
        self.negotiated_mechs: Optional[list[str]] = None
        self.auth_ctx: Optional[_AuthContext] = None

        # The pool's generation changes with each reset() so we can close
        # sockets created before the last reset.
        self.pool_gen = pool.gen
        self.generation = self.pool_gen.get_overall()
        self.ready = False
        self.cancel_context: _CancellationContext = _CancellationContext()
        self.opts = pool.opts
        self.more_to_come: bool = False
        # For load balancer support.
        self.service_id: Optional[ObjectId] = None
        self.server_connection_id: Optional[int] = None
        # When executing a transaction in load balancing mode, this flag is
        # set to true to indicate that the session now owns the connection.
        self.pinned_txn = False
        self.pinned_cursor = False
        self.active = False
        self.last_timeout = self.opts.socket_timeout
        self.connect_rtt = 0.0
        self._client_id = pool._client_id
        self.creation_time = time.monotonic()

    def set_conn_timeout(self, timeout: Optional[float]) -> None:
        """Cache last timeout to avoid duplicate calls to conn.settimeout."""
        if timeout == self.last_timeout:
            return
        self.last_timeout = timeout
        self.conn.settimeout(timeout)

    def apply_timeout(
        self, client: MongoClient, cmd: Optional[MutableMapping[str, Any]]
    ) -> Optional[float]:
        # CSOT: use remaining timeout when set.
        timeout = _csot.remaining()
        if timeout is None:
            # Reset the socket timeout unless we're performing a streaming monitor check.
            if not self.more_to_come:
                self.set_conn_timeout(self.opts.socket_timeout)
            return None
        # RTT validation.
        rtt = _csot.get_rtt()
        if rtt is None:
            rtt = self.connect_rtt
        max_time_ms = timeout - rtt
        if max_time_ms < 0:
            timeout_details = _get_timeout_details(self.opts)
            formatted = format_timeout_details(timeout_details)
            # CSOT: raise an error without running the command since we know it will time out.
            errmsg = f"operation would exceed time limit, remaining timeout:{timeout:.5f} <= network round trip time:{rtt:.5f} {formatted}"
            raise ExecutionTimeout(
                errmsg,
                50,
                {"ok": 0, "errmsg": errmsg, "code": 50},
                self.max_wire_version,
            )
        if cmd is not None:
            cmd["maxTimeMS"] = int(max_time_ms * 1000)
        self.set_conn_timeout(timeout)
        return timeout

    def pin_txn(self) -> None:
        self.pinned_txn = True
        assert not self.pinned_cursor

    def pin_cursor(self) -> None:
        self.pinned_cursor = True
        assert not self.pinned_txn

    def unpin(self) -> None:
        pool = self.pool_ref()
        if pool:
            pool.checkin(self)
        else:
            self.close_conn(ConnectionClosedReason.STALE)

    def hello_cmd(self) -> dict[str, Any]:
        # Handshake spec requires us to use OP_MSG+hello command for the
        # initial handshake in load balanced or stable API mode.
        if self.opts.server_api or self.hello_ok or self.opts.load_balanced:
            self.op_msg_enabled = True
            return {HelloCompat.CMD: 1}
        else:
            return {HelloCompat.LEGACY_CMD: 1, "helloOk": True}

    def hello(self) -> Hello[dict[str, Any]]:
        return self._hello(None, None, None)

    def _hello(
        self,
        cluster_time: Optional[ClusterTime],
        topology_version: Optional[Any],
        heartbeat_frequency: Optional[int],
    ) -> Hello[dict[str, Any]]:
        cmd = self.hello_cmd()
        performing_handshake = not self.performed_handshake
        awaitable = False
        if performing_handshake:
            self.performed_handshake = True
            cmd["client"] = self.opts.metadata
            if self.compression_settings:
                cmd["compression"] = self.compression_settings.compressors
            if self.opts.load_balanced:
                cmd["loadBalanced"] = True
        elif topology_version is not None:
            cmd["topologyVersion"] = topology_version
            assert heartbeat_frequency is not None
            cmd["maxAwaitTimeMS"] = int(heartbeat_frequency * 1000)
            awaitable = True
            # If connect_timeout is None there is no timeout.
            if self.opts.connect_timeout:
                self.set_conn_timeout(self.opts.connect_timeout + heartbeat_frequency)

        if not performing_handshake and cluster_time is not None:
            cmd["$clusterTime"] = cluster_time

        creds = self.opts._credentials
        if creds:
            if creds.mechanism == "DEFAULT" and creds.username:
                cmd["saslSupportedMechs"] = creds.source + "." + creds.username
            from pymongo import auth

            auth_ctx = auth._AuthContext.from_credentials(creds, self.address)
            if auth_ctx:
                speculative_authenticate = auth_ctx.speculate_command()
                if speculative_authenticate is not None:
                    cmd["speculativeAuthenticate"] = speculative_authenticate
        else:
            auth_ctx = None

        if performing_handshake:
            start = time.monotonic()
        doc = self.command("admin", cmd, publish_events=False, exhaust_allowed=awaitable)
        if performing_handshake:
            self.connect_rtt = time.monotonic() - start
        hello = Hello(doc, awaitable=awaitable)
        self.is_writable = hello.is_writable
        self.max_wire_version = hello.max_wire_version
        self.max_bson_size = hello.max_bson_size
        self.max_message_size = hello.max_message_size
        self.max_write_batch_size = hello.max_write_batch_size
        self.supports_sessions = (
            hello.logical_session_timeout_minutes is not None and hello.is_readable
        )
        self.logical_session_timeout_minutes: Optional[int] = hello.logical_session_timeout_minutes
        self.hello_ok = hello.hello_ok
        self.is_repl = hello.server_type in (
            SERVER_TYPE.RSPrimary,
            SERVER_TYPE.RSSecondary,
            SERVER_TYPE.RSArbiter,
            SERVER_TYPE.RSOther,
            SERVER_TYPE.RSGhost,
        )
        self.is_standalone = hello.server_type == SERVER_TYPE.Standalone
        self.is_mongos = hello.server_type == SERVER_TYPE.Mongos
        if performing_handshake and self.compression_settings:
            ctx = self.compression_settings.get_compression_context(hello.compressors)
            self.compression_context = ctx

        self.op_msg_enabled = True
        self.server_connection_id = hello.connection_id
        if creds:
            self.negotiated_mechs = hello.sasl_supported_mechs
        if auth_ctx:
            auth_ctx.parse_response(hello)  # type:ignore[arg-type]
            if auth_ctx.speculate_succeeded():
                self.auth_ctx = auth_ctx
        if self.opts.load_balanced:
            if not hello.service_id:
                raise ConfigurationError(
                    "Driver attempted to initialize in load balancing mode,"
                    " but the server does not support this mode"
                )
            self.service_id = hello.service_id
            self.generation = self.pool_gen.get(self.service_id)
        return hello

    def _next_reply(self) -> dict[str, Any]:
        reply = self.receive_message(None)
        self.more_to_come = reply.more_to_come
        unpacked_docs = reply.unpack_response()
        response_doc = unpacked_docs[0]
        helpers._check_command_response(response_doc, self.max_wire_version)
        return response_doc

    @_handle_reauth
    def command(
        self,
        dbname: str,
        spec: MutableMapping[str, Any],
        read_preference: _ServerMode = ReadPreference.PRIMARY,
        codec_options: CodecOptions = DEFAULT_CODEC_OPTIONS,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_concern: Optional[ReadConcern] = None,
        write_concern: Optional[WriteConcern] = None,
        parse_write_concern_error: bool = False,
        collation: Optional[_CollationIn] = None,
        session: Optional[ClientSession] = None,
        client: Optional[MongoClient] = None,
        retryable_write: bool = False,
        publish_events: bool = True,
        user_fields: Optional[Mapping[str, Any]] = None,
        exhaust_allowed: bool = False,
    ) -> dict[str, Any]:
        """Execute a command or raise an error.

        :param dbname: name of the database on which to run the command
        :param spec: a command document as a dict, SON, or mapping object
        :param read_preference: a read preference
        :param codec_options: a CodecOptions instance
        :param check: raise OperationFailure if there are errors
        :param allowable_errors: errors to ignore if `check` is True
        :param read_concern: The read concern for this command.
        :param write_concern: The write concern for this command.
        :param parse_write_concern_error: Whether to parse the
            ``writeConcernError`` field in the command response.
        :param collation: The collation for this command.
        :param session: optional ClientSession instance.
        :param client: optional MongoClient for gossipping $clusterTime.
        :param retryable_write: True if this command is a retryable write.
        :param publish_events: Should we publish events for this command?
        :param user_fields: Response fields that should be decoded
            using the TypeDecoders from codec_options, passed to
            bson._decode_all_selective.
        """
        self.validate_session(client, session)
        session = _validate_session_write_concern(session, write_concern)

        # Ensure command name remains in first place.
        if not isinstance(spec, ORDERED_TYPES):  # type:ignore[arg-type]
            spec = dict(spec)

        if not (write_concern is None or write_concern.acknowledged or collation is None):
            raise ConfigurationError("Collation is unsupported for unacknowledged writes.")

        self.add_server_api(spec)
        if session:
            session._apply_to(spec, retryable_write, read_preference, self)
        self.send_cluster_time(spec, session, client)
        listeners = self.listeners if publish_events else None
        unacknowledged = bool(write_concern and not write_concern.acknowledged)
        if self.op_msg_enabled:
            self._raise_if_not_writable(unacknowledged)
        try:
            return command(
                self,
                dbname,
                spec,
                self.is_mongos,
                read_preference,
                codec_options,
                session,
                client,
                check,
                allowable_errors,
                self.address,
                listeners,
                self.max_bson_size,
                read_concern,
                parse_write_concern_error=parse_write_concern_error,
                collation=collation,
                compression_ctx=self.compression_context,
                use_op_msg=self.op_msg_enabled,
                unacknowledged=unacknowledged,
                user_fields=user_fields,
                exhaust_allowed=exhaust_allowed,
                write_concern=write_concern,
            )
        except (OperationFailure, NotPrimaryError):
            raise
        # Catch socket.error, KeyboardInterrupt, etc. and close ourselves.
        except BaseException as error:
            self._raise_connection_failure(error)

    def send_message(self, message: bytes, max_doc_size: int) -> None:
        """Send a raw BSON message or raise ConnectionFailure.

        If a network exception is raised, the socket is closed.
        """
        if self.max_bson_size is not None and max_doc_size > self.max_bson_size:
            raise DocumentTooLarge(
                "BSON document too large (%d bytes) - the connected server "
                "supports BSON document sizes up to %d bytes." % (max_doc_size, self.max_bson_size)
            )

        try:
            self.conn.sendall(message)
        except BaseException as error:
            self._raise_connection_failure(error)

    def receive_message(self, request_id: Optional[int]) -> Union[_OpReply, _OpMsg]:
        """Receive a raw BSON message or raise ConnectionFailure.

        If any exception is raised, the socket is closed.
        """
        try:
            return receive_message(self, request_id, self.max_message_size)
        except BaseException as error:
            self._raise_connection_failure(error)

    def _raise_if_not_writable(self, unacknowledged: bool) -> None:
        """Raise NotPrimaryError on unacknowledged write if this socket is not
        writable.
        """
        if unacknowledged and not self.is_writable:
            # Write won't succeed, bail as if we'd received a not primary error.
            raise NotPrimaryError("not primary", {"ok": 0, "errmsg": "not primary", "code": 10107})

    def unack_write(self, msg: bytes, max_doc_size: int) -> None:
        """Send unack OP_MSG.

        Can raise ConnectionFailure or InvalidDocument.

        :param msg: bytes, an OP_MSG message.
        :param max_doc_size: size in bytes of the largest document in `msg`.
        """
        self._raise_if_not_writable(True)
        self.send_message(msg, max_doc_size)

    def write_command(
        self, request_id: int, msg: bytes, codec_options: CodecOptions
    ) -> dict[str, Any]:
        """Send "insert" etc. command, returning response as a dict.

        Can raise ConnectionFailure or OperationFailure.

        :param request_id: an int.
        :param msg: bytes, the command message.
        """
        self.send_message(msg, 0)
        reply = self.receive_message(request_id)
        result = reply.command_response(codec_options)

        # Raises NotPrimaryError or OperationFailure.
        helpers._check_command_response(result, self.max_wire_version)
        return result

    def authenticate(self, reauthenticate: bool = False) -> None:
        """Authenticate to the server if needed.

        Can raise ConnectionFailure or OperationFailure.
        """
        # CMAP spec says to publish the ready event only after authenticating
        # the connection.
        if reauthenticate:
            if self.performed_handshake:
                # Existing auth_ctx is stale, remove it.
                self.auth_ctx = None
            self.ready = False
        if not self.ready:
            creds = self.opts._credentials
            if creds:
                from pymongo import auth

                auth.authenticate(creds, self, reauthenticate=reauthenticate)
            self.ready = True
            duration = time.monotonic() - self.creation_time
            if self.enabled_for_cmap:
                assert self.listeners is not None
                self.listeners.publish_connection_ready(self.address, self.id, duration)
            if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _CONNECTION_LOGGER,
                    clientId=self._client_id,
                    message=_ConnectionStatusMessage.CONN_READY,
                    serverHost=self.address[0],
                    serverPort=self.address[1],
                    driverConnectionId=self.id,
                    durationMS=duration,
                )

    def validate_session(
        self, client: Optional[MongoClient], session: Optional[ClientSession]
    ) -> None:
        """Validate this session before use with client.

        Raises error if the client is not the one that created the session.
        """
        if session:
            if session._client is not client:
                raise InvalidOperation("Can only use session with the MongoClient that started it")

    def close_conn(self, reason: Optional[str]) -> None:
        """Close this connection with a reason."""
        if self.closed:
            return
        self._close_conn()
        if reason:
            if self.enabled_for_cmap:
                assert self.listeners is not None
                self.listeners.publish_connection_closed(self.address, self.id, reason)
            if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _CONNECTION_LOGGER,
                    clientId=self._client_id,
                    message=_ConnectionStatusMessage.CONN_CLOSED,
                    serverHost=self.address[0],
                    serverPort=self.address[1],
                    driverConnectionId=self.id,
                    reason=_verbose_connection_error_reason(reason),
                    error=reason,
                )

    def _close_conn(self) -> None:
        """Close this connection."""
        if self.closed:
            return
        self.closed = True
        self.cancel_context.cancel()
        # Note: We catch exceptions to avoid spurious errors on interpreter
        # shutdown.
        try:
            self.conn.close()
        except Exception:  # noqa: S110
            pass

    def conn_closed(self) -> bool:
        """Return True if we know socket has been closed, False otherwise."""
        return self.socket_checker.socket_closed(self.conn)

    def send_cluster_time(
        self,
        command: MutableMapping[str, Any],
        session: Optional[ClientSession],
        client: Optional[MongoClient],
    ) -> None:
        """Add $clusterTime."""
        if client:
            client._send_cluster_time(command, session)

    def add_server_api(self, command: MutableMapping[str, Any]) -> None:
        """Add server_api parameters."""
        if self.opts.server_api:
            _add_to_command(command, self.opts.server_api)

    def update_last_checkin_time(self) -> None:
        self.last_checkin_time = time.monotonic()

    def update_is_writable(self, is_writable: bool) -> None:
        self.is_writable = is_writable

    def idle_time_seconds(self) -> float:
        """Seconds since this socket was last checked into its pool."""
        return time.monotonic() - self.last_checkin_time

    def _raise_connection_failure(self, error: BaseException) -> NoReturn:
        # Catch *all* exceptions from socket methods and close the socket. In
        # regular Python, socket operations only raise socket.error, even if
        # the underlying cause was a Ctrl-C: a signal raised during socket.recv
        # is expressed as an EINTR error from poll. See internal_select_ex() in
        # socketmodule.c. All error codes from poll become socket.error at
        # first. Eventually in PyEval_EvalFrameEx the interpreter checks for
        # signals and throws KeyboardInterrupt into the current frame on the
        # main thread.
        #
        # But in Gevent and Eventlet, the polling mechanism (epoll, kqueue,
        # ..) is called in Python code, which experiences the signal as a
        # KeyboardInterrupt from the start, rather than as an initial
        # socket.error, so we catch that, close the socket, and reraise it.
        #
        # The connection closed event will be emitted later in checkin.
        if self.ready:
            reason = None
        else:
            reason = ConnectionClosedReason.ERROR
        self.close_conn(reason)
        # SSLError from PyOpenSSL inherits directly from Exception.
        if isinstance(error, (IOError, OSError, SSLError)):
            details = _get_timeout_details(self.opts)
            _raise_connection_failure(self.address, error, timeout_details=details)
        else:
            raise

    def __eq__(self, other: Any) -> bool:
        return self.conn == other.conn

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash(self.conn)

    def __repr__(self) -> str:
        return "Connection({}){} at {}".format(
            repr(self.conn),
            self.closed and " CLOSED" or "",
            id(self),
        )


def _create_connection(address: _Address, options: PoolOptions) -> socket.socket:
    """Given (host, port) and PoolOptions, connect and return a socket object.

    Can raise socket.error.

    This is a modified version of create_connection from CPython >= 2.7.
    """
    host, port = address

    # Check if dealing with a unix domain socket
    if host.endswith(".sock"):
        if not hasattr(socket, "AF_UNIX"):
            raise ConnectionFailure("UNIX-sockets are not supported on this system")
        sock = socket.socket(socket.AF_UNIX)
        # SOCK_CLOEXEC not supported for Unix sockets.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.connect(host)
            return sock
        except OSError:
            sock.close()
            raise

    # Don't try IPv6 if we don't support it. Also skip it if host
    # is 'localhost' (::1 is fine). Avoids slow connect issues
    # like PYTHON-356.
    family = socket.AF_INET
    if socket.has_ipv6 and host != "localhost":
        family = socket.AF_UNSPEC

    err = None
    for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
        af, socktype, proto, dummy, sa = res
        # SOCK_CLOEXEC was new in CPython 3.2, and only available on a limited
        # number of platforms (newer Linux and *BSD). Starting with CPython 3.4
        # all file descriptors are created non-inheritable. See PEP 446.
        try:
            sock = socket.socket(af, socktype | getattr(socket, "SOCK_CLOEXEC", 0), proto)
        except OSError:
            # Can SOCK_CLOEXEC be defined even if the kernel doesn't support
            # it?
            sock = socket.socket(af, socktype, proto)
        # Fallback when SOCK_CLOEXEC isn't available.
        _set_non_inheritable_non_atomic(sock.fileno())
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # CSOT: apply timeout to socket connect.
            timeout = _csot.remaining()
            if timeout is None:
                timeout = options.connect_timeout
            elif timeout <= 0:
                raise socket.timeout("timed out")
            sock.settimeout(timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            _set_keepalive_times(sock)
            sock.connect(sa)
            return sock
        except OSError as e:
            err = e
            sock.close()

    if err is not None:
        raise err
    else:
        # This likely means we tried to connect to an IPv6 only
        # host with an OS/kernel or Python interpreter that doesn't
        # support IPv6. The test case is Jython2.5.1 which doesn't
        # support IPv6 at all.
        raise OSError("getaddrinfo failed")


def _configured_socket(address: _Address, options: PoolOptions) -> Union[socket.socket, _sslConn]:
    """Given (host, port) and PoolOptions, return a configured socket.

    Can raise socket.error, ConnectionFailure, or _CertificateError.

    Sets socket's SSL and timeout options.
    """
    sock = _create_connection(address, options)
    ssl_context = options._ssl_context

    if ssl_context is None:
        sock.settimeout(options.socket_timeout)
        return sock

    host = address[0]
    try:
        # We have to pass hostname / ip address to wrap_socket
        # to use SSLContext.check_hostname.
        if HAS_SNI:
            ssl_sock = ssl_context.wrap_socket(sock, server_hostname=host)
        else:
            ssl_sock = ssl_context.wrap_socket(sock)
    except _CertificateError:
        sock.close()
        # Raise _CertificateError directly like we do after match_hostname
        # below.
        raise
    except (OSError, SSLError) as exc:
        sock.close()
        # We raise AutoReconnect for transient and permanent SSL handshake
        # failures alike. Permanent handshake failures, like protocol
        # mismatch, will be turned into ServerSelectionTimeoutErrors later.
        details = _get_timeout_details(options)
        _raise_connection_failure(address, exc, "SSL handshake failed: ", timeout_details=details)
    if (
        ssl_context.verify_mode
        and not ssl_context.check_hostname
        and not options.tls_allow_invalid_hostnames
    ):
        try:
            ssl.match_hostname(ssl_sock.getpeercert(), hostname=host)
        except _CertificateError:
            ssl_sock.close()
            raise

    ssl_sock.settimeout(options.socket_timeout)
    return ssl_sock


class _PoolClosedError(PyMongoError):
    """Internal error raised when a thread tries to get a connection from a
    closed pool.
    """


class _PoolGeneration:
    def __init__(self) -> None:
        # Maps service_id to generation.
        self._generations: dict[ObjectId, int] = collections.defaultdict(int)
        # Overall pool generation.
        self._generation = 0

    def get(self, service_id: Optional[ObjectId]) -> int:
        """Get the generation for the given service_id."""
        if service_id is None:
            return self._generation
        return self._generations[service_id]

    def get_overall(self) -> int:
        """Get the Pool's overall generation."""
        return self._generation

    def inc(self, service_id: Optional[ObjectId]) -> None:
        """Increment the generation for the given service_id."""
        self._generation += 1
        if service_id is None:
            for service_id in self._generations:
                self._generations[service_id] += 1
        else:
            self._generations[service_id] += 1

    def stale(self, gen: int, service_id: Optional[ObjectId]) -> bool:
        """Return if the given generation for a given service_id is stale."""
        return gen != self.get(service_id)


class PoolState:
    PAUSED = 1
    READY = 2
    CLOSED = 3


# Do *not* explicitly inherit from object or Jython won't call __del__
# http://bugs.jython.org/issue1057
class Pool:
    def __init__(
        self,
        address: _Address,
        options: PoolOptions,
        handshake: bool = True,
        client_id: Optional[ObjectId] = None,
    ):
        """
        :param address: a (hostname, port) tuple
        :param options: a PoolOptions instance
        :param handshake: whether to call hello for each new Connection
        """
        if options.pause_enabled:
            self.state = PoolState.PAUSED
        else:
            self.state = PoolState.READY
        # Check a socket's health with socket_closed() every once in a while.
        # Can override for testing: 0 to always check, None to never check.
        self._check_interval_seconds = 1
        # LIFO pool. Sockets are ordered on idle time. Sockets claimed
        # and returned to pool from the left side. Stale sockets removed
        # from the right side.
        self.conns: collections.deque = collections.deque()
        self.active_contexts: set[_CancellationContext] = set()
        self.lock = _create_lock()
        self.active_sockets = 0
        # Monotonically increasing connection ID required for CMAP Events.
        self.next_connection_id = 1
        # Track whether the sockets in this pool are writeable or not.
        self.is_writable: Optional[bool] = None

        # Keep track of resets, so we notice sockets created before the most
        # recent reset and close them.
        # self.generation = 0
        self.gen = _PoolGeneration()
        self.pid = os.getpid()
        self.address = address
        self.opts = options
        self.handshake = handshake
        # Don't publish events in Monitor pools.
        self.enabled_for_cmap = (
            self.handshake
            and self.opts._event_listeners is not None
            and self.opts._event_listeners.enabled_for_cmap
        )
        self.enabled_for_logging = self.handshake

        # The first portion of the wait queue.
        # Enforces: maxPoolSize
        # Also used for: clearing the wait queue
        self.size_cond = threading.Condition(self.lock)
        self.requests = 0
        self.max_pool_size = self.opts.max_pool_size
        if not self.max_pool_size:
            self.max_pool_size = float("inf")
        # The second portion of the wait queue.
        # Enforces: maxConnecting
        # Also used for: clearing the wait queue
        self._max_connecting_cond = threading.Condition(self.lock)
        self._max_connecting = self.opts.max_connecting
        self._pending = 0
        self._client_id = client_id
        if self.enabled_for_cmap:
            assert self.opts._event_listeners is not None
            self.opts._event_listeners.publish_pool_created(
                self.address, self.opts.non_default_options
            )
        if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _CONNECTION_LOGGER,
                clientId=self._client_id,
                message=_ConnectionStatusMessage.POOL_CREATED,
                serverHost=self.address[0],
                serverPort=self.address[1],
                **self.opts.non_default_options,
            )
        # Similar to active_sockets but includes threads in the wait queue.
        self.operation_count: int = 0
        # Retain references to pinned connections to prevent the CPython GC
        # from thinking that a cursor's pinned connection can be GC'd when the
        # cursor is GC'd (see PYTHON-2751).
        self.__pinned_sockets: set[Connection] = set()
        self.ncursors = 0
        self.ntxns = 0

    def ready(self) -> None:
        # Take the lock to avoid the race condition described in PYTHON-2699.
        with self.lock:
            if self.state != PoolState.READY:
                self.state = PoolState.READY
                if self.enabled_for_cmap:
                    assert self.opts._event_listeners is not None
                    self.opts._event_listeners.publish_pool_ready(self.address)
                if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _CONNECTION_LOGGER,
                        clientId=self._client_id,
                        message=_ConnectionStatusMessage.POOL_READY,
                        serverHost=self.address[0],
                        serverPort=self.address[1],
                    )

    @property
    def closed(self) -> bool:
        return self.state == PoolState.CLOSED

    def _reset(
        self,
        close: bool,
        pause: bool = True,
        service_id: Optional[ObjectId] = None,
        interrupt_connections: bool = False,
    ) -> None:
        old_state = self.state
        with self.size_cond:
            if self.closed:
                return
            if self.opts.pause_enabled and pause and not self.opts.load_balanced:
                old_state, self.state = self.state, PoolState.PAUSED
            self.gen.inc(service_id)
            newpid = os.getpid()
            if self.pid != newpid:
                self.pid = newpid
                self.active_sockets = 0
                self.operation_count = 0
            if service_id is None:
                sockets, self.conns = self.conns, collections.deque()
            else:
                discard: collections.deque = collections.deque()
                keep: collections.deque = collections.deque()
                for conn in self.conns:
                    if conn.service_id == service_id:
                        discard.append(conn)
                    else:
                        keep.append(conn)
                sockets = discard
                self.conns = keep

            if close:
                self.state = PoolState.CLOSED
            # Clear the wait queue
            self._max_connecting_cond.notify_all()
            self.size_cond.notify_all()

            if interrupt_connections:
                for context in self.active_contexts:
                    context.cancel()

        listeners = self.opts._event_listeners
        # CMAP spec says that close() MUST close sockets before publishing the
        # PoolClosedEvent but that reset() SHOULD close sockets *after*
        # publishing the PoolClearedEvent.
        if close:
            for conn in sockets:
                conn.close_conn(ConnectionClosedReason.POOL_CLOSED)
            if self.enabled_for_cmap:
                assert listeners is not None
                listeners.publish_pool_closed(self.address)
            if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _CONNECTION_LOGGER,
                    clientId=self._client_id,
                    message=_ConnectionStatusMessage.POOL_CLOSED,
                    serverHost=self.address[0],
                    serverPort=self.address[1],
                )
        else:
            if old_state != PoolState.PAUSED:
                if self.enabled_for_cmap:
                    assert listeners is not None
                    listeners.publish_pool_cleared(
                        self.address,
                        service_id=service_id,
                        interrupt_connections=interrupt_connections,
                    )
                if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _CONNECTION_LOGGER,
                        clientId=self._client_id,
                        message=_ConnectionStatusMessage.POOL_CLEARED,
                        serverHost=self.address[0],
                        serverPort=self.address[1],
                        serviceId=service_id,
                    )
            for conn in sockets:
                conn.close_conn(ConnectionClosedReason.STALE)

    def update_is_writable(self, is_writable: Optional[bool]) -> None:
        """Updates the is_writable attribute on all sockets currently in the
        Pool.
        """
        self.is_writable = is_writable
        with self.lock:
            for _socket in self.conns:
                _socket.update_is_writable(self.is_writable)

    def reset(
        self, service_id: Optional[ObjectId] = None, interrupt_connections: bool = False
    ) -> None:
        self._reset(close=False, service_id=service_id, interrupt_connections=interrupt_connections)

    def reset_without_pause(self) -> None:
        self._reset(close=False, pause=False)

    def close(self) -> None:
        self._reset(close=True)

    def stale_generation(self, gen: int, service_id: Optional[ObjectId]) -> bool:
        return self.gen.stale(gen, service_id)

    def remove_stale_sockets(self, reference_generation: int) -> None:
        """Removes stale sockets then adds new ones if pool is too small and
        has not been reset. The `reference_generation` argument specifies the
        `generation` at the point in time this operation was requested on the
        pool.
        """
        # Take the lock to avoid the race condition described in PYTHON-2699.
        with self.lock:
            if self.state != PoolState.READY:
                return

        if self.opts.max_idle_time_seconds is not None:
            with self.lock:
                while (
                    self.conns
                    and self.conns[-1].idle_time_seconds() > self.opts.max_idle_time_seconds
                ):
                    conn = self.conns.pop()
                    conn.close_conn(ConnectionClosedReason.IDLE)

        while True:
            with self.size_cond:
                # There are enough sockets in the pool.
                if len(self.conns) + self.active_sockets >= self.opts.min_pool_size:
                    return
                if self.requests >= self.opts.min_pool_size:
                    return
                self.requests += 1
            incremented = False
            try:
                with self._max_connecting_cond:
                    # If maxConnecting connections are already being created
                    # by this pool then try again later instead of waiting.
                    if self._pending >= self._max_connecting:
                        return
                    self._pending += 1
                    incremented = True
                conn = self.connect()
                with self.lock:
                    # Close connection and return if the pool was reset during
                    # socket creation or while acquiring the pool lock.
                    if self.gen.get_overall() != reference_generation:
                        conn.close_conn(ConnectionClosedReason.STALE)
                        return
                    self.conns.appendleft(conn)
                    self.active_contexts.discard(conn.cancel_context)
            finally:
                if incremented:
                    # Notify after adding the socket to the pool.
                    with self._max_connecting_cond:
                        self._pending -= 1
                        self._max_connecting_cond.notify()

                with self.size_cond:
                    self.requests -= 1
                    self.size_cond.notify()

    def connect(self, handler: Optional[_MongoClientErrorHandler] = None) -> Connection:
        """Connect to Mongo and return a new Connection.

        Can raise ConnectionFailure.

        Note that the pool does not keep a reference to the socket -- you
        must call checkin() when you're done with it.
        """
        with self.lock:
            conn_id = self.next_connection_id
            self.next_connection_id += 1

        listeners = self.opts._event_listeners
        if self.enabled_for_cmap:
            assert listeners is not None
            listeners.publish_connection_created(self.address, conn_id)
        if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _CONNECTION_LOGGER,
                clientId=self._client_id,
                message=_ConnectionStatusMessage.CONN_CREATED,
                serverHost=self.address[0],
                serverPort=self.address[1],
                driverConnectionId=conn_id,
            )

        try:
            sock = _configured_socket(self.address, self.opts)
        except BaseException as error:
            if self.enabled_for_cmap:
                assert listeners is not None
                listeners.publish_connection_closed(
                    self.address, conn_id, ConnectionClosedReason.ERROR
                )
            if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _CONNECTION_LOGGER,
                    clientId=self._client_id,
                    message=_ConnectionStatusMessage.CONN_CLOSED,
                    serverHost=self.address[0],
                    serverPort=self.address[1],
                    driverConnectionId=conn_id,
                    reason=_verbose_connection_error_reason(ConnectionClosedReason.ERROR),
                    error=ConnectionClosedReason.ERROR,
                )
            if isinstance(error, (IOError, OSError, SSLError)):
                details = _get_timeout_details(self.opts)
                _raise_connection_failure(self.address, error, timeout_details=details)

            raise

        conn = Connection(sock, self, self.address, conn_id)  # type: ignore[arg-type]
        with self.lock:
            self.active_contexts.add(conn.cancel_context)
        try:
            if self.handshake:
                conn.hello()
                self.is_writable = conn.is_writable
            if handler:
                handler.contribute_socket(conn, completed_handshake=False)

            conn.authenticate()
        except BaseException:
            conn.close_conn(ConnectionClosedReason.ERROR)
            raise

        return conn

    @contextlib.contextmanager
    def checkout(self, handler: Optional[_MongoClientErrorHandler] = None) -> Iterator[Connection]:
        """Get a connection from the pool. Use with a "with" statement.

        Returns a :class:`Connection` object wrapping a connected
        :class:`socket.socket`.

        This method should always be used in a with-statement::

            with pool.get_conn() as connection:
                connection.send_message(msg)
                data = connection.receive_message(op_code, request_id)

        Can raise ConnectionFailure or OperationFailure.

        :param handler: A _MongoClientErrorHandler.
        """
        listeners = self.opts._event_listeners
        checkout_started_time = time.monotonic()
        if self.enabled_for_cmap:
            assert listeners is not None
            listeners.publish_connection_check_out_started(self.address)
        if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _CONNECTION_LOGGER,
                clientId=self._client_id,
                message=_ConnectionStatusMessage.CHECKOUT_STARTED,
                serverHost=self.address[0],
                serverPort=self.address[1],
            )

        conn = self._get_conn(checkout_started_time, handler=handler)

        duration = time.monotonic() - checkout_started_time
        if self.enabled_for_cmap:
            assert listeners is not None
            listeners.publish_connection_checked_out(self.address, conn.id, duration)
        if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _CONNECTION_LOGGER,
                clientId=self._client_id,
                message=_ConnectionStatusMessage.CHECKOUT_SUCCEEDED,
                serverHost=self.address[0],
                serverPort=self.address[1],
                driverConnectionId=conn.id,
                durationMS=duration,
            )
        try:
            with self.lock:
                self.active_contexts.add(conn.cancel_context)
            yield conn
        except BaseException:
            # Exception in caller. Ensure the connection gets returned.
            # Note that when pinned is True, the session owns the
            # connection and it is responsible for checking the connection
            # back into the pool.
            pinned = conn.pinned_txn or conn.pinned_cursor
            if handler:
                # Perform SDAM error handling rules while the connection is
                # still checked out.
                exc_type, exc_val, _ = sys.exc_info()
                handler.handle(exc_type, exc_val)
            if not pinned and conn.active:
                self.checkin(conn)
            raise
        if conn.pinned_txn:
            with self.lock:
                self.__pinned_sockets.add(conn)
                self.ntxns += 1
        elif conn.pinned_cursor:
            with self.lock:
                self.__pinned_sockets.add(conn)
                self.ncursors += 1
        elif conn.active:
            self.checkin(conn)

    def _raise_if_not_ready(self, checkout_started_time: float, emit_event: bool) -> None:
        if self.state != PoolState.READY:
            if emit_event:
                duration = time.monotonic() - checkout_started_time
                if self.enabled_for_cmap:
                    assert self.opts._event_listeners is not None
                    self.opts._event_listeners.publish_connection_check_out_failed(
                        self.address, ConnectionCheckOutFailedReason.CONN_ERROR, duration
                    )
                if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _CONNECTION_LOGGER,
                        clientId=self._client_id,
                        message=_ConnectionStatusMessage.CHECKOUT_FAILED,
                        serverHost=self.address[0],
                        serverPort=self.address[1],
                        reason="An error occurred while trying to establish a new connection",
                        error=ConnectionCheckOutFailedReason.CONN_ERROR,
                        durationMS=duration,
                    )

            details = _get_timeout_details(self.opts)
            _raise_connection_failure(
                self.address, AutoReconnect("connection pool paused"), timeout_details=details
            )

    def _get_conn(
        self, checkout_started_time: float, handler: Optional[_MongoClientErrorHandler] = None
    ) -> Connection:
        """Get or create a Connection. Can raise ConnectionFailure."""
        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_client:TestClient.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self.reset_without_pause()

        if self.closed:
            duration = time.monotonic() - checkout_started_time
            if self.enabled_for_cmap:
                assert self.opts._event_listeners is not None
                self.opts._event_listeners.publish_connection_check_out_failed(
                    self.address, ConnectionCheckOutFailedReason.POOL_CLOSED, duration
                )
            if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                _debug_log(
                    _CONNECTION_LOGGER,
                    clientId=self._client_id,
                    message=_ConnectionStatusMessage.CHECKOUT_FAILED,
                    serverHost=self.address[0],
                    serverPort=self.address[1],
                    reason="Connection pool was closed",
                    error=ConnectionCheckOutFailedReason.POOL_CLOSED,
                    durationMS=duration,
                )
            raise _PoolClosedError(
                "Attempted to check out a connection from closed connection pool"
            )

        with self.lock:
            self.operation_count += 1

        # Get a free socket or create one.
        if _csot.get_timeout():
            deadline = _csot.get_deadline()
        elif self.opts.wait_queue_timeout:
            deadline = time.monotonic() + self.opts.wait_queue_timeout
        else:
            deadline = None

        with self.size_cond:
            self._raise_if_not_ready(checkout_started_time, emit_event=True)
            while not (self.requests < self.max_pool_size):
                if not _cond_wait(self.size_cond, deadline):
                    # Timed out, notify the next thread to ensure a
                    # timeout doesn't consume the condition.
                    if self.requests < self.max_pool_size:
                        self.size_cond.notify()
                    self._raise_wait_queue_timeout(checkout_started_time)
                self._raise_if_not_ready(checkout_started_time, emit_event=True)
            self.requests += 1

        # We've now acquired the semaphore and must release it on error.
        conn = None
        incremented = False
        emitted_event = False
        try:
            with self.lock:
                self.active_sockets += 1
                incremented = True
            while conn is None:
                # CMAP: we MUST wait for either maxConnecting OR for a socket
                # to be checked back into the pool.
                with self._max_connecting_cond:
                    self._raise_if_not_ready(checkout_started_time, emit_event=False)
                    while not (self.conns or self._pending < self._max_connecting):
                        if not _cond_wait(self._max_connecting_cond, deadline):
                            # Timed out, notify the next thread to ensure a
                            # timeout doesn't consume the condition.
                            if self.conns or self._pending < self._max_connecting:
                                self._max_connecting_cond.notify()
                            emitted_event = True
                            self._raise_wait_queue_timeout(checkout_started_time)
                        self._raise_if_not_ready(checkout_started_time, emit_event=False)

                    try:
                        conn = self.conns.popleft()
                    except IndexError:
                        self._pending += 1
                if conn:  # We got a socket from the pool
                    if self._perished(conn):
                        conn = None
                        continue
                else:  # We need to create a new connection
                    try:
                        conn = self.connect(handler=handler)
                    finally:
                        with self._max_connecting_cond:
                            self._pending -= 1
                            self._max_connecting_cond.notify()
        except BaseException:
            if conn:
                # We checked out a socket but authentication failed.
                conn.close_conn(ConnectionClosedReason.ERROR)
            with self.size_cond:
                self.requests -= 1
                if incremented:
                    self.active_sockets -= 1
                self.size_cond.notify()

            if not emitted_event:
                duration = time.monotonic() - checkout_started_time
                if self.enabled_for_cmap:
                    assert self.opts._event_listeners is not None
                    self.opts._event_listeners.publish_connection_check_out_failed(
                        self.address, ConnectionCheckOutFailedReason.CONN_ERROR, duration
                    )
                if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _CONNECTION_LOGGER,
                        clientId=self._client_id,
                        message=_ConnectionStatusMessage.CHECKOUT_FAILED,
                        serverHost=self.address[0],
                        serverPort=self.address[1],
                        reason="An error occurred while trying to establish a new connection",
                        error=ConnectionCheckOutFailedReason.CONN_ERROR,
                        durationMS=duration,
                    )
            raise

        conn.active = True
        return conn

    def checkin(self, conn: Connection) -> None:
        """Return the connection to the pool, or if it's closed discard it.

        :param conn: The connection to check into the pool.
        """
        txn = conn.pinned_txn
        cursor = conn.pinned_cursor
        conn.active = False
        conn.pinned_txn = False
        conn.pinned_cursor = False
        self.__pinned_sockets.discard(conn)
        listeners = self.opts._event_listeners
        with self.lock:
            self.active_contexts.discard(conn.cancel_context)
        if self.enabled_for_cmap:
            assert listeners is not None
            listeners.publish_connection_checked_in(self.address, conn.id)
        if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _CONNECTION_LOGGER,
                clientId=self._client_id,
                message=_ConnectionStatusMessage.CHECKEDIN,
                serverHost=self.address[0],
                serverPort=self.address[1],
                driverConnectionId=conn.id,
            )
        if self.pid != os.getpid():
            self.reset_without_pause()
        else:
            if self.closed:
                conn.close_conn(ConnectionClosedReason.POOL_CLOSED)
            elif conn.closed:
                # CMAP requires the closed event be emitted after the check in.
                if self.enabled_for_cmap:
                    assert listeners is not None
                    listeners.publish_connection_closed(
                        self.address, conn.id, ConnectionClosedReason.ERROR
                    )
                if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _CONNECTION_LOGGER,
                        clientId=self._client_id,
                        message=_ConnectionStatusMessage.CONN_CLOSED,
                        serverHost=self.address[0],
                        serverPort=self.address[1],
                        driverConnectionId=conn.id,
                        reason=_verbose_connection_error_reason(ConnectionClosedReason.ERROR),
                        error=ConnectionClosedReason.ERROR,
                    )
            else:
                with self.lock:
                    # Hold the lock to ensure this section does not race with
                    # Pool.reset().
                    if self.stale_generation(conn.generation, conn.service_id):
                        conn.close_conn(ConnectionClosedReason.STALE)
                    else:
                        conn.update_last_checkin_time()
                        conn.update_is_writable(bool(self.is_writable))
                        self.conns.appendleft(conn)
                        # Notify any threads waiting to create a connection.
                        self._max_connecting_cond.notify()

        with self.size_cond:
            if txn:
                self.ntxns -= 1
            elif cursor:
                self.ncursors -= 1
            self.requests -= 1
            self.active_sockets -= 1
            self.operation_count -= 1
            self.size_cond.notify()

    def _perished(self, conn: Connection) -> bool:
        """Return True and close the connection if it is "perished".

        This side-effecty function checks if this socket has been idle for
        for longer than the max idle time, or if the socket has been closed by
        some external network error, or if the socket's generation is outdated.

        Checking sockets lets us avoid seeing *some*
        :class:`~pymongo.errors.AutoReconnect` exceptions on server
        hiccups, etc. We only check if the socket was closed by an external
        error if it has been > 1 second since the socket was checked into the
        pool, to keep performance reasonable - we can't avoid AutoReconnects
        completely anyway.
        """
        idle_time_seconds = conn.idle_time_seconds()
        # If socket is idle, open a new one.
        if (
            self.opts.max_idle_time_seconds is not None
            and idle_time_seconds > self.opts.max_idle_time_seconds
        ):
            conn.close_conn(ConnectionClosedReason.IDLE)
            return True

        if self._check_interval_seconds is not None and (
            self._check_interval_seconds == 0 or idle_time_seconds > self._check_interval_seconds
        ):
            if conn.conn_closed():
                conn.close_conn(ConnectionClosedReason.ERROR)
                return True

        if self.stale_generation(conn.generation, conn.service_id):
            conn.close_conn(ConnectionClosedReason.STALE)
            return True

        return False

    def _raise_wait_queue_timeout(self, checkout_started_time: float) -> NoReturn:
        listeners = self.opts._event_listeners
        duration = time.monotonic() - checkout_started_time
        if self.enabled_for_cmap:
            assert listeners is not None
            listeners.publish_connection_check_out_failed(
                self.address, ConnectionCheckOutFailedReason.TIMEOUT, duration
            )
        if self.enabled_for_logging and _CONNECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _CONNECTION_LOGGER,
                clientId=self._client_id,
                message=_ConnectionStatusMessage.CHECKOUT_FAILED,
                serverHost=self.address[0],
                serverPort=self.address[1],
                reason="Wait queue timeout elapsed without a connection becoming available",
                error=ConnectionCheckOutFailedReason.TIMEOUT,
                durationMS=duration,
            )
        timeout = _csot.get_timeout() or self.opts.wait_queue_timeout
        if self.opts.load_balanced:
            other_ops = self.active_sockets - self.ncursors - self.ntxns
            raise WaitQueueTimeoutError(
                "Timeout waiting for connection from the connection pool. "
                "maxPoolSize: {}, connections in use by cursors: {}, "
                "connections in use by transactions: {}, connections in use "
                "by other operations: {}, timeout: {}".format(
                    self.opts.max_pool_size,
                    self.ncursors,
                    self.ntxns,
                    other_ops,
                    timeout,
                )
            )
        raise WaitQueueTimeoutError(
            "Timed out while checking out a connection from connection pool. "
            f"maxPoolSize: {self.opts.max_pool_size}, timeout: {timeout}"
        )

    def __del__(self) -> None:
        # Avoid ResourceWarnings in Python 3
        # Close all sockets without calling reset() or close() because it is
        # not safe to acquire a lock in __del__.
        for conn in self.conns:
            conn.close_conn(None)

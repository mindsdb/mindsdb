# Copyright 2015-present MongoDB, Inc.
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

"""Tools to monitor driver events.

.. versionadded:: 3.1

.. attention:: Starting in PyMongo 3.11, the monitoring classes outlined below
    are included in the PyMongo distribution under the
    :mod:`~pymongo.event_loggers` submodule.

Use :func:`register` to register global listeners for specific events.
Listeners must inherit from one of the abstract classes below and implement
the correct functions for that class.

For example, a simple command logger might be implemented like this::

    import logging

    from pymongo import monitoring

    class CommandLogger(monitoring.CommandListener):

        def started(self, event):
            logging.info("Command {0.command_name} with request id "
                         "{0.request_id} started on server "
                         "{0.connection_id}".format(event))

        def succeeded(self, event):
            logging.info("Command {0.command_name} with request id "
                         "{0.request_id} on server {0.connection_id} "
                         "succeeded in {0.duration_micros} "
                         "microseconds".format(event))

        def failed(self, event):
            logging.info("Command {0.command_name} with request id "
                         "{0.request_id} on server {0.connection_id} "
                         "failed in {0.duration_micros} "
                         "microseconds".format(event))

    monitoring.register(CommandLogger())

Server discovery and monitoring events are also available. For example::

    class ServerLogger(monitoring.ServerListener):

        def opened(self, event):
            logging.info("Server {0.server_address} added to topology "
                         "{0.topology_id}".format(event))

        def description_changed(self, event):
            previous_server_type = event.previous_description.server_type
            new_server_type = event.new_description.server_type
            if new_server_type != previous_server_type:
                # server_type_name was added in PyMongo 3.4
                logging.info(
                    "Server {0.server_address} changed type from "
                    "{0.previous_description.server_type_name} to "
                    "{0.new_description.server_type_name}".format(event))

        def closed(self, event):
            logging.warning("Server {0.server_address} removed from topology "
                            "{0.topology_id}".format(event))


    class HeartbeatLogger(monitoring.ServerHeartbeatListener):

        def started(self, event):
            logging.info("Heartbeat sent to server "
                         "{0.connection_id}".format(event))

        def succeeded(self, event):
            # The reply.document attribute was added in PyMongo 3.4.
            logging.info("Heartbeat to server {0.connection_id} "
                         "succeeded with reply "
                         "{0.reply.document}".format(event))

        def failed(self, event):
            logging.warning("Heartbeat to server {0.connection_id} "
                            "failed with error {0.reply}".format(event))

    class TopologyLogger(monitoring.TopologyListener):

        def opened(self, event):
            logging.info("Topology with id {0.topology_id} "
                         "opened".format(event))

        def description_changed(self, event):
            logging.info("Topology description updated for "
                         "topology id {0.topology_id}".format(event))
            previous_topology_type = event.previous_description.topology_type
            new_topology_type = event.new_description.topology_type
            if new_topology_type != previous_topology_type:
                # topology_type_name was added in PyMongo 3.4
                logging.info(
                    "Topology {0.topology_id} changed type from "
                    "{0.previous_description.topology_type_name} to "
                    "{0.new_description.topology_type_name}".format(event))
            # The has_writable_server and has_readable_server methods
            # were added in PyMongo 3.4.
            if not event.new_description.has_writable_server():
                logging.warning("No writable servers available.")
            if not event.new_description.has_readable_server():
                logging.warning("No readable servers available.")

        def closed(self, event):
            logging.info("Topology with id {0.topology_id} "
                         "closed".format(event))

Connection monitoring and pooling events are also available. For example::

    class ConnectionPoolLogger(ConnectionPoolListener):

        def pool_created(self, event):
            logging.info("[pool {0.address}] pool created".format(event))

        def pool_ready(self, event):
            logging.info("[pool {0.address}] pool is ready".format(event))

        def pool_cleared(self, event):
            logging.info("[pool {0.address}] pool cleared".format(event))

        def pool_closed(self, event):
            logging.info("[pool {0.address}] pool closed".format(event))

        def connection_created(self, event):
            logging.info("[pool {0.address}][connection #{0.connection_id}] "
                         "connection created".format(event))

        def connection_ready(self, event):
            logging.info("[pool {0.address}][connection #{0.connection_id}] "
                         "connection setup succeeded".format(event))

        def connection_closed(self, event):
            logging.info("[pool {0.address}][connection #{0.connection_id}] "
                         "connection closed, reason: "
                         "{0.reason}".format(event))

        def connection_check_out_started(self, event):
            logging.info("[pool {0.address}] connection check out "
                         "started".format(event))

        def connection_check_out_failed(self, event):
            logging.info("[pool {0.address}] connection check out "
                         "failed, reason: {0.reason}".format(event))

        def connection_checked_out(self, event):
            logging.info("[pool {0.address}][connection #{0.connection_id}] "
                         "connection checked out of pool".format(event))

        def connection_checked_in(self, event):
            logging.info("[pool {0.address}][connection #{0.connection_id}] "
                         "connection checked into pool".format(event))


Event listeners can also be registered per instance of
:class:`~pymongo.mongo_client.MongoClient`::

    client = MongoClient(event_listeners=[CommandLogger()])

Note that previously registered global listeners are automatically included
when configuring per client event listeners. Registering a new global listener
will not add that listener to existing client instances.

.. note:: Events are delivered **synchronously**. Application threads block
  waiting for event handlers (e.g. :meth:`~CommandListener.started`) to
  return. Care must be taken to ensure that your event handlers are efficient
  enough to not adversely affect overall application performance.

.. warning:: The command documents published through this API are *not* copies.
  If you intend to modify them in any way you must copy them in your event
  handler first.
"""

from __future__ import annotations

import datetime
from collections import abc, namedtuple
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

from bson.objectid import ObjectId
from pymongo.hello import Hello, HelloCompat
from pymongo.helpers import _SENSITIVE_COMMANDS, _handle_exception
from pymongo.typings import _Address, _DocumentOut

if TYPE_CHECKING:
    from datetime import timedelta

    from pymongo.server_description import ServerDescription
    from pymongo.topology_description import TopologyDescription


_Listeners = namedtuple(
    "_Listeners",
    (
        "command_listeners",
        "server_listeners",
        "server_heartbeat_listeners",
        "topology_listeners",
        "cmap_listeners",
    ),
)

_LISTENERS = _Listeners([], [], [], [], [])


class _EventListener:
    """Abstract base class for all event listeners."""


class CommandListener(_EventListener):
    """Abstract base class for command listeners.

    Handles `CommandStartedEvent`, `CommandSucceededEvent`,
    and `CommandFailedEvent`.
    """

    def started(self, event: CommandStartedEvent) -> None:
        """Abstract method to handle a `CommandStartedEvent`.

        :param event: An instance of :class:`CommandStartedEvent`.
        """
        raise NotImplementedError

    def succeeded(self, event: CommandSucceededEvent) -> None:
        """Abstract method to handle a `CommandSucceededEvent`.

        :param event: An instance of :class:`CommandSucceededEvent`.
        """
        raise NotImplementedError

    def failed(self, event: CommandFailedEvent) -> None:
        """Abstract method to handle a `CommandFailedEvent`.

        :param event: An instance of :class:`CommandFailedEvent`.
        """
        raise NotImplementedError


class ConnectionPoolListener(_EventListener):
    """Abstract base class for connection pool listeners.

    Handles all of the connection pool events defined in the Connection
    Monitoring and Pooling Specification:
    :class:`PoolCreatedEvent`, :class:`PoolClearedEvent`,
    :class:`PoolClosedEvent`, :class:`ConnectionCreatedEvent`,
    :class:`ConnectionReadyEvent`, :class:`ConnectionClosedEvent`,
    :class:`ConnectionCheckOutStartedEvent`,
    :class:`ConnectionCheckOutFailedEvent`,
    :class:`ConnectionCheckedOutEvent`,
    and :class:`ConnectionCheckedInEvent`.

    .. versionadded:: 3.9
    """

    def pool_created(self, event: PoolCreatedEvent) -> None:
        """Abstract method to handle a :class:`PoolCreatedEvent`.

        Emitted when a connection Pool is created.

        :param event: An instance of :class:`PoolCreatedEvent`.
        """
        raise NotImplementedError

    def pool_ready(self, event: PoolReadyEvent) -> None:
        """Abstract method to handle a :class:`PoolReadyEvent`.

        Emitted when a connection Pool is marked ready.

        :param event: An instance of :class:`PoolReadyEvent`.

        .. versionadded:: 4.0
        """
        raise NotImplementedError

    def pool_cleared(self, event: PoolClearedEvent) -> None:
        """Abstract method to handle a `PoolClearedEvent`.

        Emitted when a connection Pool is cleared.

        :param event: An instance of :class:`PoolClearedEvent`.
        """
        raise NotImplementedError

    def pool_closed(self, event: PoolClosedEvent) -> None:
        """Abstract method to handle a `PoolClosedEvent`.

        Emitted when a connection Pool is closed.

        :param event: An instance of :class:`PoolClosedEvent`.
        """
        raise NotImplementedError

    def connection_created(self, event: ConnectionCreatedEvent) -> None:
        """Abstract method to handle a :class:`ConnectionCreatedEvent`.

        Emitted when a connection Pool creates a Connection object.

        :param event: An instance of :class:`ConnectionCreatedEvent`.
        """
        raise NotImplementedError

    def connection_ready(self, event: ConnectionReadyEvent) -> None:
        """Abstract method to handle a :class:`ConnectionReadyEvent`.

        Emitted when a connection has finished its setup, and is now ready to
        use.

        :param event: An instance of :class:`ConnectionReadyEvent`.
        """
        raise NotImplementedError

    def connection_closed(self, event: ConnectionClosedEvent) -> None:
        """Abstract method to handle a :class:`ConnectionClosedEvent`.

        Emitted when a connection Pool closes a connection.

        :param event: An instance of :class:`ConnectionClosedEvent`.
        """
        raise NotImplementedError

    def connection_check_out_started(self, event: ConnectionCheckOutStartedEvent) -> None:
        """Abstract method to handle a :class:`ConnectionCheckOutStartedEvent`.

        Emitted when the driver starts attempting to check out a connection.

        :param event: An instance of :class:`ConnectionCheckOutStartedEvent`.
        """
        raise NotImplementedError

    def connection_check_out_failed(self, event: ConnectionCheckOutFailedEvent) -> None:
        """Abstract method to handle a :class:`ConnectionCheckOutFailedEvent`.

        Emitted when the driver's attempt to check out a connection fails.

        :param event: An instance of :class:`ConnectionCheckOutFailedEvent`.
        """
        raise NotImplementedError

    def connection_checked_out(self, event: ConnectionCheckedOutEvent) -> None:
        """Abstract method to handle a :class:`ConnectionCheckedOutEvent`.

        Emitted when the driver successfully checks out a connection.

        :param event: An instance of :class:`ConnectionCheckedOutEvent`.
        """
        raise NotImplementedError

    def connection_checked_in(self, event: ConnectionCheckedInEvent) -> None:
        """Abstract method to handle a :class:`ConnectionCheckedInEvent`.

        Emitted when the driver checks in a connection back to the connection
        Pool.

        :param event: An instance of :class:`ConnectionCheckedInEvent`.
        """
        raise NotImplementedError


class ServerHeartbeatListener(_EventListener):
    """Abstract base class for server heartbeat listeners.

    Handles `ServerHeartbeatStartedEvent`, `ServerHeartbeatSucceededEvent`,
    and `ServerHeartbeatFailedEvent`.

    .. versionadded:: 3.3
    """

    def started(self, event: ServerHeartbeatStartedEvent) -> None:
        """Abstract method to handle a `ServerHeartbeatStartedEvent`.

        :param event: An instance of :class:`ServerHeartbeatStartedEvent`.
        """
        raise NotImplementedError

    def succeeded(self, event: ServerHeartbeatSucceededEvent) -> None:
        """Abstract method to handle a `ServerHeartbeatSucceededEvent`.

        :param event: An instance of :class:`ServerHeartbeatSucceededEvent`.
        """
        raise NotImplementedError

    def failed(self, event: ServerHeartbeatFailedEvent) -> None:
        """Abstract method to handle a `ServerHeartbeatFailedEvent`.

        :param event: An instance of :class:`ServerHeartbeatFailedEvent`.
        """
        raise NotImplementedError


class TopologyListener(_EventListener):
    """Abstract base class for topology monitoring listeners.
    Handles `TopologyOpenedEvent`, `TopologyDescriptionChangedEvent`, and
    `TopologyClosedEvent`.

    .. versionadded:: 3.3
    """

    def opened(self, event: TopologyOpenedEvent) -> None:
        """Abstract method to handle a `TopologyOpenedEvent`.

        :param event: An instance of :class:`TopologyOpenedEvent`.
        """
        raise NotImplementedError

    def description_changed(self, event: TopologyDescriptionChangedEvent) -> None:
        """Abstract method to handle a `TopologyDescriptionChangedEvent`.

        :param event: An instance of :class:`TopologyDescriptionChangedEvent`.
        """
        raise NotImplementedError

    def closed(self, event: TopologyClosedEvent) -> None:
        """Abstract method to handle a `TopologyClosedEvent`.

        :param event: An instance of :class:`TopologyClosedEvent`.
        """
        raise NotImplementedError


class ServerListener(_EventListener):
    """Abstract base class for server listeners.
    Handles `ServerOpeningEvent`, `ServerDescriptionChangedEvent`, and
    `ServerClosedEvent`.

    .. versionadded:: 3.3
    """

    def opened(self, event: ServerOpeningEvent) -> None:
        """Abstract method to handle a `ServerOpeningEvent`.

        :param event: An instance of :class:`ServerOpeningEvent`.
        """
        raise NotImplementedError

    def description_changed(self, event: ServerDescriptionChangedEvent) -> None:
        """Abstract method to handle a `ServerDescriptionChangedEvent`.

        :param event: An instance of :class:`ServerDescriptionChangedEvent`.
        """
        raise NotImplementedError

    def closed(self, event: ServerClosedEvent) -> None:
        """Abstract method to handle a `ServerClosedEvent`.

        :param event: An instance of :class:`ServerClosedEvent`.
        """
        raise NotImplementedError


def _to_micros(dur: timedelta) -> int:
    """Convert duration 'dur' to microseconds."""
    return int(dur.total_seconds() * 10e5)


def _validate_event_listeners(
    option: str, listeners: Sequence[_EventListeners]
) -> Sequence[_EventListeners]:
    """Validate event listeners"""
    if not isinstance(listeners, abc.Sequence):
        raise TypeError(f"{option} must be a list or tuple")
    for listener in listeners:
        if not isinstance(listener, _EventListener):
            raise TypeError(
                f"Listeners for {option} must be either a "
                "CommandListener, ServerHeartbeatListener, "
                "ServerListener, TopologyListener, or "
                "ConnectionPoolListener."
            )
    return listeners


def register(listener: _EventListener) -> None:
    """Register a global event listener.

    :param listener: A subclasses of :class:`CommandListener`,
        :class:`ServerHeartbeatListener`, :class:`ServerListener`,
        :class:`TopologyListener`, or :class:`ConnectionPoolListener`.
    """
    if not isinstance(listener, _EventListener):
        raise TypeError(
            f"Listeners for {listener} must be either a "
            "CommandListener, ServerHeartbeatListener, "
            "ServerListener, TopologyListener, or "
            "ConnectionPoolListener."
        )
    if isinstance(listener, CommandListener):
        _LISTENERS.command_listeners.append(listener)
    if isinstance(listener, ServerHeartbeatListener):
        _LISTENERS.server_heartbeat_listeners.append(listener)
    if isinstance(listener, ServerListener):
        _LISTENERS.server_listeners.append(listener)
    if isinstance(listener, TopologyListener):
        _LISTENERS.topology_listeners.append(listener)
    if isinstance(listener, ConnectionPoolListener):
        _LISTENERS.cmap_listeners.append(listener)


# The "hello" command is also deemed sensitive when attempting speculative
# authentication.
def _is_speculative_authenticate(command_name: str, doc: Mapping[str, Any]) -> bool:
    if (
        command_name.lower() in ("hello", HelloCompat.LEGACY_CMD)
        and "speculativeAuthenticate" in doc
    ):
        return True
    return False


class _CommandEvent:
    """Base class for command events."""

    __slots__ = (
        "__cmd_name",
        "__rqst_id",
        "__conn_id",
        "__op_id",
        "__service_id",
        "__db",
        "__server_conn_id",
    )

    def __init__(
        self,
        command_name: str,
        request_id: int,
        connection_id: _Address,
        operation_id: Optional[int],
        service_id: Optional[ObjectId] = None,
        database_name: str = "",
        server_connection_id: Optional[int] = None,
    ) -> None:
        self.__cmd_name = command_name
        self.__rqst_id = request_id
        self.__conn_id = connection_id
        self.__op_id = operation_id
        self.__service_id = service_id
        self.__db = database_name
        self.__server_conn_id = server_connection_id

    @property
    def command_name(self) -> str:
        """The command name."""
        return self.__cmd_name

    @property
    def request_id(self) -> int:
        """The request id for this operation."""
        return self.__rqst_id

    @property
    def connection_id(self) -> _Address:
        """The address (host, port) of the server this command was sent to."""
        return self.__conn_id

    @property
    def service_id(self) -> Optional[ObjectId]:
        """The service_id this command was sent to, or ``None``.

        .. versionadded:: 3.12
        """
        return self.__service_id

    @property
    def operation_id(self) -> Optional[int]:
        """An id for this series of events or None."""
        return self.__op_id

    @property
    def database_name(self) -> str:
        """The database_name this command was sent to, or ``""``.

        .. versionadded:: 4.6
        """
        return self.__db

    @property
    def server_connection_id(self) -> Optional[int]:
        """The server-side connection id for the connection this command was sent on, or ``None``.

        .. versionadded:: 4.7
        """
        return self.__server_conn_id


class CommandStartedEvent(_CommandEvent):
    """Event published when a command starts.

    :param command: The command document.
    :param database_name: The name of the database this command was run against.
    :param request_id: The request id for this operation.
    :param connection_id: The address (host, port) of the server this command
        was sent to.
    :param operation_id: An optional identifier for a series of related events.
    :param service_id: The service_id this command was sent to, or ``None``.
    """

    __slots__ = ("__cmd",)

    def __init__(
        self,
        command: _DocumentOut,
        database_name: str,
        request_id: int,
        connection_id: _Address,
        operation_id: Optional[int],
        service_id: Optional[ObjectId] = None,
        server_connection_id: Optional[int] = None,
    ) -> None:
        if not command:
            raise ValueError(f"{command!r} is not a valid command")
        # Command name must be first key.
        command_name = next(iter(command))
        super().__init__(
            command_name,
            request_id,
            connection_id,
            operation_id,
            service_id=service_id,
            database_name=database_name,
            server_connection_id=server_connection_id,
        )
        cmd_name = command_name.lower()
        if cmd_name in _SENSITIVE_COMMANDS or _is_speculative_authenticate(cmd_name, command):
            self.__cmd: _DocumentOut = {}
        else:
            self.__cmd = command

    @property
    def command(self) -> _DocumentOut:
        """The command document."""
        return self.__cmd

    @property
    def database_name(self) -> str:
        """The name of the database this command was run against."""
        return super().database_name

    def __repr__(self) -> str:
        return (
            "<{} {} db: {!r}, command: {!r}, operation_id: {}, service_id: {}, server_connection_id: {}>"
        ).format(
            self.__class__.__name__,
            self.connection_id,
            self.database_name,
            self.command_name,
            self.operation_id,
            self.service_id,
            self.server_connection_id,
        )


class CommandSucceededEvent(_CommandEvent):
    """Event published when a command succeeds.

    :param duration: The command duration as a datetime.timedelta.
    :param reply: The server reply document.
    :param command_name: The command name.
    :param request_id: The request id for this operation.
    :param connection_id: The address (host, port) of the server this command
        was sent to.
    :param operation_id: An optional identifier for a series of related events.
    :param service_id: The service_id this command was sent to, or ``None``.
    :param database_name: The database this command was sent to, or ``""``.
    """

    __slots__ = ("__duration_micros", "__reply")

    def __init__(
        self,
        duration: datetime.timedelta,
        reply: _DocumentOut,
        command_name: str,
        request_id: int,
        connection_id: _Address,
        operation_id: Optional[int],
        service_id: Optional[ObjectId] = None,
        database_name: str = "",
        server_connection_id: Optional[int] = None,
    ) -> None:
        super().__init__(
            command_name,
            request_id,
            connection_id,
            operation_id,
            service_id=service_id,
            database_name=database_name,
            server_connection_id=server_connection_id,
        )
        self.__duration_micros = _to_micros(duration)
        cmd_name = command_name.lower()
        if cmd_name in _SENSITIVE_COMMANDS or _is_speculative_authenticate(cmd_name, reply):
            self.__reply: _DocumentOut = {}
        else:
            self.__reply = reply

    @property
    def duration_micros(self) -> int:
        """The duration of this operation in microseconds."""
        return self.__duration_micros

    @property
    def reply(self) -> _DocumentOut:
        """The server failure document for this operation."""
        return self.__reply

    def __repr__(self) -> str:
        return (
            "<{} {} db: {!r}, command: {!r}, operation_id: {}, duration_micros: {}, service_id: {}, server_connection_id: {}>"
        ).format(
            self.__class__.__name__,
            self.connection_id,
            self.database_name,
            self.command_name,
            self.operation_id,
            self.duration_micros,
            self.service_id,
            self.server_connection_id,
        )


class CommandFailedEvent(_CommandEvent):
    """Event published when a command fails.

    :param duration: The command duration as a datetime.timedelta.
    :param failure: The server reply document.
    :param command_name: The command name.
    :param request_id: The request id for this operation.
    :param connection_id: The address (host, port) of the server this command
        was sent to.
    :param operation_id: An optional identifier for a series of related events.
    :param service_id: The service_id this command was sent to, or ``None``.
    :param database_name: The database this command was sent to, or ``""``.
    """

    __slots__ = ("__duration_micros", "__failure")

    def __init__(
        self,
        duration: datetime.timedelta,
        failure: _DocumentOut,
        command_name: str,
        request_id: int,
        connection_id: _Address,
        operation_id: Optional[int],
        service_id: Optional[ObjectId] = None,
        database_name: str = "",
        server_connection_id: Optional[int] = None,
    ) -> None:
        super().__init__(
            command_name,
            request_id,
            connection_id,
            operation_id,
            service_id=service_id,
            database_name=database_name,
            server_connection_id=server_connection_id,
        )
        self.__duration_micros = _to_micros(duration)
        self.__failure = failure

    @property
    def duration_micros(self) -> int:
        """The duration of this operation in microseconds."""
        return self.__duration_micros

    @property
    def failure(self) -> _DocumentOut:
        """The server failure document for this operation."""
        return self.__failure

    def __repr__(self) -> str:
        return (
            "<{} {} db: {!r}, command: {!r}, operation_id: {}, duration_micros: {}, "
            "failure: {!r}, service_id: {}, server_connection_id: {}>"
        ).format(
            self.__class__.__name__,
            self.connection_id,
            self.database_name,
            self.command_name,
            self.operation_id,
            self.duration_micros,
            self.failure,
            self.service_id,
            self.server_connection_id,
        )


class _PoolEvent:
    """Base class for pool events."""

    __slots__ = ("__address",)

    def __init__(self, address: _Address) -> None:
        self.__address = address

    @property
    def address(self) -> _Address:
        """The address (host, port) pair of the server the pool is attempting
        to connect to.
        """
        return self.__address

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__address!r})"


class PoolCreatedEvent(_PoolEvent):
    """Published when a Connection Pool is created.

    :param address: The address (host, port) pair of the server this Pool is
       attempting to connect to.

    .. versionadded:: 3.9
    """

    __slots__ = ("__options",)

    def __init__(self, address: _Address, options: dict[str, Any]) -> None:
        super().__init__(address)
        self.__options = options

    @property
    def options(self) -> dict[str, Any]:
        """Any non-default pool options that were set on this Connection Pool."""
        return self.__options

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.address!r}, {self.__options!r})"


class PoolReadyEvent(_PoolEvent):
    """Published when a Connection Pool is marked ready.

    :param address: The address (host, port) pair of the server this Pool is
       attempting to connect to.

    .. versionadded:: 4.0
    """

    __slots__ = ()


class PoolClearedEvent(_PoolEvent):
    """Published when a Connection Pool is cleared.

    :param address: The address (host, port) pair of the server this Pool is
       attempting to connect to.
    :param service_id: The service_id this command was sent to, or ``None``.
    :param interrupt_connections: True if all active connections were interrupted by the Pool during clearing.

    .. versionadded:: 3.9
    """

    __slots__ = ("__service_id", "__interrupt_connections")

    def __init__(
        self,
        address: _Address,
        service_id: Optional[ObjectId] = None,
        interrupt_connections: bool = False,
    ) -> None:
        super().__init__(address)
        self.__service_id = service_id
        self.__interrupt_connections = interrupt_connections

    @property
    def service_id(self) -> Optional[ObjectId]:
        """Connections with this service_id are cleared.

        When service_id is ``None``, all connections in the pool are cleared.

        .. versionadded:: 3.12
        """
        return self.__service_id

    @property
    def interrupt_connections(self) -> bool:
        """If True, active connections are interrupted during clearing.

        .. versionadded:: 4.7
        """
        return self.__interrupt_connections

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.address!r}, {self.__service_id!r}, {self.__interrupt_connections!r})"


class PoolClosedEvent(_PoolEvent):
    """Published when a Connection Pool is closed.

    :param address: The address (host, port) pair of the server this Pool is
       attempting to connect to.

    .. versionadded:: 3.9
    """

    __slots__ = ()


class ConnectionClosedReason:
    """An enum that defines values for `reason` on a
    :class:`ConnectionClosedEvent`.

    .. versionadded:: 3.9
    """

    STALE = "stale"
    """The pool was cleared, making the connection no longer valid."""

    IDLE = "idle"
    """The connection became stale by being idle for too long (maxIdleTimeMS).
    """

    ERROR = "error"
    """The connection experienced an error, making it no longer valid."""

    POOL_CLOSED = "poolClosed"
    """The pool was closed, making the connection no longer valid."""


class ConnectionCheckOutFailedReason:
    """An enum that defines values for `reason` on a
    :class:`ConnectionCheckOutFailedEvent`.

    .. versionadded:: 3.9
    """

    TIMEOUT = "timeout"
    """The connection check out attempt exceeded the specified timeout."""

    POOL_CLOSED = "poolClosed"
    """The pool was previously closed, and cannot provide new connections."""

    CONN_ERROR = "connectionError"
    """The connection check out attempt experienced an error while setting up
    a new connection.
    """


class _ConnectionEvent:
    """Private base class for connection events."""

    __slots__ = ("__address",)

    def __init__(self, address: _Address) -> None:
        self.__address = address

    @property
    def address(self) -> _Address:
        """The address (host, port) pair of the server this connection is
        attempting to connect to.
        """
        return self.__address

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__address!r})"


class _ConnectionIdEvent(_ConnectionEvent):
    """Private base class for connection events with an id."""

    __slots__ = ("__connection_id",)

    def __init__(self, address: _Address, connection_id: int) -> None:
        super().__init__(address)
        self.__connection_id = connection_id

    @property
    def connection_id(self) -> int:
        """The ID of the connection."""
        return self.__connection_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.address!r}, {self.__connection_id!r})"


class _ConnectionDurationEvent(_ConnectionIdEvent):
    """Private base class for connection events with a duration."""

    __slots__ = ("__duration",)

    def __init__(self, address: _Address, connection_id: int, duration: Optional[float]) -> None:
        super().__init__(address, connection_id)
        self.__duration = duration

    @property
    def duration(self) -> Optional[float]:
        """The duration of the connection event.

        .. versionadded:: 4.7
        """
        return self.__duration

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.address!r}, {self.connection_id!r}, {self.__duration!r})"


class ConnectionCreatedEvent(_ConnectionIdEvent):
    """Published when a Connection Pool creates a Connection object.

    NOTE: This connection is not ready for use until the
    :class:`ConnectionReadyEvent` is published.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.
    :param connection_id: The integer ID of the Connection in this Pool.

    .. versionadded:: 3.9
    """

    __slots__ = ()


class ConnectionReadyEvent(_ConnectionDurationEvent):
    """Published when a Connection has finished its setup, and is ready to use.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.
    :param connection_id: The integer ID of the Connection in this Pool.

    .. versionadded:: 3.9
    """

    __slots__ = ()


class ConnectionClosedEvent(_ConnectionIdEvent):
    """Published when a Connection is closed.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.
    :param connection_id: The integer ID of the Connection in this Pool.
    :param reason: A reason explaining why this connection was closed.

    .. versionadded:: 3.9
    """

    __slots__ = ("__reason",)

    def __init__(self, address: _Address, connection_id: int, reason: str):
        super().__init__(address, connection_id)
        self.__reason = reason

    @property
    def reason(self) -> str:
        """A reason explaining why this connection was closed.

        The reason must be one of the strings from the
        :class:`ConnectionClosedReason` enum.
        """
        return self.__reason

    def __repr__(self) -> str:
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__,
            self.address,
            self.connection_id,
            self.__reason,
        )


class ConnectionCheckOutStartedEvent(_ConnectionEvent):
    """Published when the driver starts attempting to check out a connection.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.

    .. versionadded:: 3.9
    """

    __slots__ = ()


class ConnectionCheckOutFailedEvent(_ConnectionDurationEvent):
    """Published when the driver's attempt to check out a connection fails.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.
    :param reason: A reason explaining why connection check out failed.

    .. versionadded:: 3.9
    """

    __slots__ = ("__reason",)

    def __init__(self, address: _Address, reason: str, duration: Optional[float]) -> None:
        super().__init__(address=address, connection_id=0, duration=duration)
        self.__reason = reason

    @property
    def reason(self) -> str:
        """A reason explaining why connection check out failed.

        The reason must be one of the strings from the
        :class:`ConnectionCheckOutFailedReason` enum.
        """
        return self.__reason

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.address!r}, {self.__reason!r}, {self.duration!r})"


class ConnectionCheckedOutEvent(_ConnectionDurationEvent):
    """Published when the driver successfully checks out a connection.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.
    :param connection_id: The integer ID of the Connection in this Pool.

    .. versionadded:: 3.9
    """

    __slots__ = ()


class ConnectionCheckedInEvent(_ConnectionIdEvent):
    """Published when the driver checks in a Connection into the Pool.

    :param address: The address (host, port) pair of the server this
       Connection is attempting to connect to.
    :param connection_id: The integer ID of the Connection in this Pool.

    .. versionadded:: 3.9
    """

    __slots__ = ()


class _ServerEvent:
    """Base class for server events."""

    __slots__ = ("__server_address", "__topology_id")

    def __init__(self, server_address: _Address, topology_id: ObjectId) -> None:
        self.__server_address = server_address
        self.__topology_id = topology_id

    @property
    def server_address(self) -> _Address:
        """The address (host, port) pair of the server"""
        return self.__server_address

    @property
    def topology_id(self) -> ObjectId:
        """A unique identifier for the topology this server is a part of."""
        return self.__topology_id

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.server_address} topology_id: {self.topology_id}>"


class ServerDescriptionChangedEvent(_ServerEvent):
    """Published when server description changes.

    .. versionadded:: 3.3
    """

    __slots__ = ("__previous_description", "__new_description")

    def __init__(
        self,
        previous_description: ServerDescription,
        new_description: ServerDescription,
        *args: Any,
    ) -> None:
        super().__init__(*args)
        self.__previous_description = previous_description
        self.__new_description = new_description

    @property
    def previous_description(self) -> ServerDescription:
        """The previous
        :class:`~pymongo.server_description.ServerDescription`.
        """
        return self.__previous_description

    @property
    def new_description(self) -> ServerDescription:
        """The new
        :class:`~pymongo.server_description.ServerDescription`.
        """
        return self.__new_description

    def __repr__(self) -> str:
        return "<{} {} changed from: {}, to: {}>".format(
            self.__class__.__name__,
            self.server_address,
            self.previous_description,
            self.new_description,
        )


class ServerOpeningEvent(_ServerEvent):
    """Published when server is initialized.

    .. versionadded:: 3.3
    """

    __slots__ = ()


class ServerClosedEvent(_ServerEvent):
    """Published when server is closed.

    .. versionadded:: 3.3
    """

    __slots__ = ()


class TopologyEvent:
    """Base class for topology description events."""

    __slots__ = ("__topology_id",)

    def __init__(self, topology_id: ObjectId) -> None:
        self.__topology_id = topology_id

    @property
    def topology_id(self) -> ObjectId:
        """A unique identifier for the topology this server is a part of."""
        return self.__topology_id

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} topology_id: {self.topology_id}>"


class TopologyDescriptionChangedEvent(TopologyEvent):
    """Published when the topology description changes.

    .. versionadded:: 3.3
    """

    __slots__ = ("__previous_description", "__new_description")

    def __init__(
        self,
        previous_description: TopologyDescription,
        new_description: TopologyDescription,
        *args: Any,
    ) -> None:
        super().__init__(*args)
        self.__previous_description = previous_description
        self.__new_description = new_description

    @property
    def previous_description(self) -> TopologyDescription:
        """The previous
        :class:`~pymongo.topology_description.TopologyDescription`.
        """
        return self.__previous_description

    @property
    def new_description(self) -> TopologyDescription:
        """The new
        :class:`~pymongo.topology_description.TopologyDescription`.
        """
        return self.__new_description

    def __repr__(self) -> str:
        return "<{} topology_id: {} changed from: {}, to: {}>".format(
            self.__class__.__name__,
            self.topology_id,
            self.previous_description,
            self.new_description,
        )


class TopologyOpenedEvent(TopologyEvent):
    """Published when the topology is initialized.

    .. versionadded:: 3.3
    """

    __slots__ = ()


class TopologyClosedEvent(TopologyEvent):
    """Published when the topology is closed.

    .. versionadded:: 3.3
    """

    __slots__ = ()


class _ServerHeartbeatEvent:
    """Base class for server heartbeat events."""

    __slots__ = ("__connection_id", "__awaited")

    def __init__(self, connection_id: _Address, awaited: bool = False) -> None:
        self.__connection_id = connection_id
        self.__awaited = awaited

    @property
    def connection_id(self) -> _Address:
        """The address (host, port) of the server this heartbeat was sent
        to.
        """
        return self.__connection_id

    @property
    def awaited(self) -> bool:
        """Whether the heartbeat was issued as an awaitable hello command.

        .. versionadded:: 4.6
        """
        return self.__awaited

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.connection_id} awaited: {self.awaited}>"


class ServerHeartbeatStartedEvent(_ServerHeartbeatEvent):
    """Published when a heartbeat is started.

    .. versionadded:: 3.3
    """

    __slots__ = ()


class ServerHeartbeatSucceededEvent(_ServerHeartbeatEvent):
    """Fired when the server heartbeat succeeds.

    .. versionadded:: 3.3
    """

    __slots__ = ("__duration", "__reply")

    def __init__(
        self, duration: float, reply: Hello, connection_id: _Address, awaited: bool = False
    ) -> None:
        super().__init__(connection_id, awaited)
        self.__duration = duration
        self.__reply = reply

    @property
    def duration(self) -> float:
        """The duration of this heartbeat in microseconds."""
        return self.__duration

    @property
    def reply(self) -> Hello:
        """An instance of :class:`~pymongo.hello.Hello`."""
        return self.__reply

    @property
    def awaited(self) -> bool:
        """Whether the heartbeat was awaited.

        If true, then :meth:`duration` reflects the sum of the round trip time
        to the server and the time that the server waited before sending a
        response.

        .. versionadded:: 3.11
        """
        return super().awaited

    def __repr__(self) -> str:
        return "<{} {} duration: {}, awaited: {}, reply: {}>".format(
            self.__class__.__name__,
            self.connection_id,
            self.duration,
            self.awaited,
            self.reply,
        )


class ServerHeartbeatFailedEvent(_ServerHeartbeatEvent):
    """Fired when the server heartbeat fails, either with an "ok: 0"
    or a socket exception.

    .. versionadded:: 3.3
    """

    __slots__ = ("__duration", "__reply")

    def __init__(
        self, duration: float, reply: Exception, connection_id: _Address, awaited: bool = False
    ) -> None:
        super().__init__(connection_id, awaited)
        self.__duration = duration
        self.__reply = reply

    @property
    def duration(self) -> float:
        """The duration of this heartbeat in microseconds."""
        return self.__duration

    @property
    def reply(self) -> Exception:
        """A subclass of :exc:`Exception`."""
        return self.__reply

    @property
    def awaited(self) -> bool:
        """Whether the heartbeat was awaited.

        If true, then :meth:`duration` reflects the sum of the round trip time
        to the server and the time that the server waited before sending a
        response.

        .. versionadded:: 3.11
        """
        return super().awaited

    def __repr__(self) -> str:
        return "<{} {} duration: {}, awaited: {}, reply: {!r}>".format(
            self.__class__.__name__,
            self.connection_id,
            self.duration,
            self.awaited,
            self.reply,
        )


class _EventListeners:
    """Configure event listeners for a client instance.

    Any event listeners registered globally are included by default.

    :param listeners: A list of event listeners.
    """

    def __init__(self, listeners: Optional[Sequence[_EventListener]]):
        self.__command_listeners = _LISTENERS.command_listeners[:]
        self.__server_listeners = _LISTENERS.server_listeners[:]
        lst = _LISTENERS.server_heartbeat_listeners
        self.__server_heartbeat_listeners = lst[:]
        self.__topology_listeners = _LISTENERS.topology_listeners[:]
        self.__cmap_listeners = _LISTENERS.cmap_listeners[:]
        if listeners is not None:
            for lst in listeners:
                if isinstance(lst, CommandListener):
                    self.__command_listeners.append(lst)
                if isinstance(lst, ServerListener):
                    self.__server_listeners.append(lst)
                if isinstance(lst, ServerHeartbeatListener):
                    self.__server_heartbeat_listeners.append(lst)
                if isinstance(lst, TopologyListener):
                    self.__topology_listeners.append(lst)
                if isinstance(lst, ConnectionPoolListener):
                    self.__cmap_listeners.append(lst)
        self.__enabled_for_commands = bool(self.__command_listeners)
        self.__enabled_for_server = bool(self.__server_listeners)
        self.__enabled_for_server_heartbeat = bool(self.__server_heartbeat_listeners)
        self.__enabled_for_topology = bool(self.__topology_listeners)
        self.__enabled_for_cmap = bool(self.__cmap_listeners)

    @property
    def enabled_for_commands(self) -> bool:
        """Are any CommandListener instances registered?"""
        return self.__enabled_for_commands

    @property
    def enabled_for_server(self) -> bool:
        """Are any ServerListener instances registered?"""
        return self.__enabled_for_server

    @property
    def enabled_for_server_heartbeat(self) -> bool:
        """Are any ServerHeartbeatListener instances registered?"""
        return self.__enabled_for_server_heartbeat

    @property
    def enabled_for_topology(self) -> bool:
        """Are any TopologyListener instances registered?"""
        return self.__enabled_for_topology

    @property
    def enabled_for_cmap(self) -> bool:
        """Are any ConnectionPoolListener instances registered?"""
        return self.__enabled_for_cmap

    def event_listeners(self) -> list[_EventListeners]:
        """List of registered event listeners."""
        return (
            self.__command_listeners
            + self.__server_heartbeat_listeners
            + self.__server_listeners
            + self.__topology_listeners
            + self.__cmap_listeners
        )

    def publish_command_start(
        self,
        command: _DocumentOut,
        database_name: str,
        request_id: int,
        connection_id: _Address,
        server_connection_id: Optional[int],
        op_id: Optional[int] = None,
        service_id: Optional[ObjectId] = None,
    ) -> None:
        """Publish a CommandStartedEvent to all command listeners.

        :param command: The command document.
        :param database_name: The name of the database this command was run
            against.
        :param request_id: The request id for this operation.
        :param connection_id: The address (host, port) of the server this
            command was sent to.
        :param op_id: The (optional) operation id for this operation.
        :param service_id: The service_id this command was sent to, or ``None``.
        """
        if op_id is None:
            op_id = request_id
        event = CommandStartedEvent(
            command,
            database_name,
            request_id,
            connection_id,
            op_id,
            service_id=service_id,
            server_connection_id=server_connection_id,
        )
        for subscriber in self.__command_listeners:
            try:
                subscriber.started(event)
            except Exception:
                _handle_exception()

    def publish_command_success(
        self,
        duration: timedelta,
        reply: _DocumentOut,
        command_name: str,
        request_id: int,
        connection_id: _Address,
        server_connection_id: Optional[int],
        op_id: Optional[int] = None,
        service_id: Optional[ObjectId] = None,
        speculative_hello: bool = False,
        database_name: str = "",
    ) -> None:
        """Publish a CommandSucceededEvent to all command listeners.

        :param duration: The command duration as a datetime.timedelta.
        :param reply: The server reply document.
        :param command_name: The command name.
        :param request_id: The request id for this operation.
        :param connection_id: The address (host, port) of the server this
            command was sent to.
        :param op_id: The (optional) operation id for this operation.
        :param service_id: The service_id this command was sent to, or ``None``.
        :param speculative_hello: Was the command sent with speculative auth?
        :param database_name: The database this command was sent to, or ``""``.
        """
        if op_id is None:
            op_id = request_id
        if speculative_hello:
            # Redact entire response when the command started contained
            # speculativeAuthenticate.
            reply = {}
        event = CommandSucceededEvent(
            duration,
            reply,
            command_name,
            request_id,
            connection_id,
            op_id,
            service_id,
            database_name=database_name,
            server_connection_id=server_connection_id,
        )
        for subscriber in self.__command_listeners:
            try:
                subscriber.succeeded(event)
            except Exception:
                _handle_exception()

    def publish_command_failure(
        self,
        duration: timedelta,
        failure: _DocumentOut,
        command_name: str,
        request_id: int,
        connection_id: _Address,
        server_connection_id: Optional[int],
        op_id: Optional[int] = None,
        service_id: Optional[ObjectId] = None,
        database_name: str = "",
    ) -> None:
        """Publish a CommandFailedEvent to all command listeners.

        :param duration: The command duration as a datetime.timedelta.
        :param failure: The server reply document or failure description
            document.
        :param command_name: The command name.
        :param request_id: The request id for this operation.
        :param connection_id: The address (host, port) of the server this
            command was sent to.
        :param op_id: The (optional) operation id for this operation.
        :param service_id: The service_id this command was sent to, or ``None``.
        :param database_name: The database this command was sent to, or ``""``.
        """
        if op_id is None:
            op_id = request_id
        event = CommandFailedEvent(
            duration,
            failure,
            command_name,
            request_id,
            connection_id,
            op_id,
            service_id=service_id,
            database_name=database_name,
            server_connection_id=server_connection_id,
        )
        for subscriber in self.__command_listeners:
            try:
                subscriber.failed(event)
            except Exception:
                _handle_exception()

    def publish_server_heartbeat_started(self, connection_id: _Address, awaited: bool) -> None:
        """Publish a ServerHeartbeatStartedEvent to all server heartbeat
        listeners.

        :param connection_id: The address (host, port) pair of the connection.
        :param awaited: True if this heartbeat is part of an awaitable hello command.
        """
        event = ServerHeartbeatStartedEvent(connection_id, awaited)
        for subscriber in self.__server_heartbeat_listeners:
            try:
                subscriber.started(event)
            except Exception:
                _handle_exception()

    def publish_server_heartbeat_succeeded(
        self, connection_id: _Address, duration: float, reply: Hello, awaited: bool
    ) -> None:
        """Publish a ServerHeartbeatSucceededEvent to all server heartbeat
        listeners.

        :param connection_id: The address (host, port) pair of the connection.
        :param duration: The execution time of the event in the highest possible
            resolution for the platform.
        :param reply: The command reply.
        :param awaited: True if the response was awaited.
        """
        event = ServerHeartbeatSucceededEvent(duration, reply, connection_id, awaited)
        for subscriber in self.__server_heartbeat_listeners:
            try:
                subscriber.succeeded(event)
            except Exception:
                _handle_exception()

    def publish_server_heartbeat_failed(
        self, connection_id: _Address, duration: float, reply: Exception, awaited: bool
    ) -> None:
        """Publish a ServerHeartbeatFailedEvent to all server heartbeat
        listeners.

        :param connection_id: The address (host, port) pair of the connection.
        :param duration: The execution time of the event in the highest possible
            resolution for the platform.
        :param reply: The command reply.
        :param awaited: True if the response was awaited.
        """
        event = ServerHeartbeatFailedEvent(duration, reply, connection_id, awaited)
        for subscriber in self.__server_heartbeat_listeners:
            try:
                subscriber.failed(event)
            except Exception:
                _handle_exception()

    def publish_server_opened(self, server_address: _Address, topology_id: ObjectId) -> None:
        """Publish a ServerOpeningEvent to all server listeners.

        :param server_address: The address (host, port) pair of the server.
        :param topology_id: A unique identifier for the topology this server
           is a part of.
        """
        event = ServerOpeningEvent(server_address, topology_id)
        for subscriber in self.__server_listeners:
            try:
                subscriber.opened(event)
            except Exception:
                _handle_exception()

    def publish_server_closed(self, server_address: _Address, topology_id: ObjectId) -> None:
        """Publish a ServerClosedEvent to all server listeners.

        :param server_address: The address (host, port) pair of the server.
        :param topology_id: A unique identifier for the topology this server
           is a part of.
        """
        event = ServerClosedEvent(server_address, topology_id)
        for subscriber in self.__server_listeners:
            try:
                subscriber.closed(event)
            except Exception:
                _handle_exception()

    def publish_server_description_changed(
        self,
        previous_description: ServerDescription,
        new_description: ServerDescription,
        server_address: _Address,
        topology_id: ObjectId,
    ) -> None:
        """Publish a ServerDescriptionChangedEvent to all server listeners.

        :param previous_description: The previous server description.
        :param server_address: The address (host, port) pair of the server.
        :param new_description: The new server description.
        :param topology_id: A unique identifier for the topology this server
           is a part of.
        """
        event = ServerDescriptionChangedEvent(
            previous_description, new_description, server_address, topology_id
        )
        for subscriber in self.__server_listeners:
            try:
                subscriber.description_changed(event)
            except Exception:
                _handle_exception()

    def publish_topology_opened(self, topology_id: ObjectId) -> None:
        """Publish a TopologyOpenedEvent to all topology listeners.

        :param topology_id: A unique identifier for the topology this server
           is a part of.
        """
        event = TopologyOpenedEvent(topology_id)
        for subscriber in self.__topology_listeners:
            try:
                subscriber.opened(event)
            except Exception:
                _handle_exception()

    def publish_topology_closed(self, topology_id: ObjectId) -> None:
        """Publish a TopologyClosedEvent to all topology listeners.

        :param topology_id: A unique identifier for the topology this server
           is a part of.
        """
        event = TopologyClosedEvent(topology_id)
        for subscriber in self.__topology_listeners:
            try:
                subscriber.closed(event)
            except Exception:
                _handle_exception()

    def publish_topology_description_changed(
        self,
        previous_description: TopologyDescription,
        new_description: TopologyDescription,
        topology_id: ObjectId,
    ) -> None:
        """Publish a TopologyDescriptionChangedEvent to all topology listeners.

        :param previous_description: The previous topology description.
        :param new_description: The new topology description.
        :param topology_id: A unique identifier for the topology this server
           is a part of.
        """
        event = TopologyDescriptionChangedEvent(previous_description, new_description, topology_id)
        for subscriber in self.__topology_listeners:
            try:
                subscriber.description_changed(event)
            except Exception:
                _handle_exception()

    def publish_pool_created(self, address: _Address, options: dict[str, Any]) -> None:
        """Publish a :class:`PoolCreatedEvent` to all pool listeners."""
        event = PoolCreatedEvent(address, options)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.pool_created(event)
            except Exception:
                _handle_exception()

    def publish_pool_ready(self, address: _Address) -> None:
        """Publish a :class:`PoolReadyEvent` to all pool listeners."""
        event = PoolReadyEvent(address)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.pool_ready(event)
            except Exception:
                _handle_exception()

    def publish_pool_cleared(
        self,
        address: _Address,
        service_id: Optional[ObjectId],
        interrupt_connections: bool = False,
    ) -> None:
        """Publish a :class:`PoolClearedEvent` to all pool listeners."""
        event = PoolClearedEvent(address, service_id, interrupt_connections)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.pool_cleared(event)
            except Exception:
                _handle_exception()

    def publish_pool_closed(self, address: _Address) -> None:
        """Publish a :class:`PoolClosedEvent` to all pool listeners."""
        event = PoolClosedEvent(address)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.pool_closed(event)
            except Exception:
                _handle_exception()

    def publish_connection_created(self, address: _Address, connection_id: int) -> None:
        """Publish a :class:`ConnectionCreatedEvent` to all connection
        listeners.
        """
        event = ConnectionCreatedEvent(address, connection_id)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_created(event)
            except Exception:
                _handle_exception()

    def publish_connection_ready(
        self, address: _Address, connection_id: int, duration: float
    ) -> None:
        """Publish a :class:`ConnectionReadyEvent` to all connection listeners."""
        event = ConnectionReadyEvent(address, connection_id, duration)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_ready(event)
            except Exception:
                _handle_exception()

    def publish_connection_closed(self, address: _Address, connection_id: int, reason: str) -> None:
        """Publish a :class:`ConnectionClosedEvent` to all connection
        listeners.
        """
        event = ConnectionClosedEvent(address, connection_id, reason)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_closed(event)
            except Exception:
                _handle_exception()

    def publish_connection_check_out_started(self, address: _Address) -> None:
        """Publish a :class:`ConnectionCheckOutStartedEvent` to all connection
        listeners.
        """
        event = ConnectionCheckOutStartedEvent(address)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_check_out_started(event)
            except Exception:
                _handle_exception()

    def publish_connection_check_out_failed(
        self, address: _Address, reason: str, duration: float
    ) -> None:
        """Publish a :class:`ConnectionCheckOutFailedEvent` to all connection
        listeners.
        """
        event = ConnectionCheckOutFailedEvent(address, reason, duration)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_check_out_failed(event)
            except Exception:
                _handle_exception()

    def publish_connection_checked_out(
        self, address: _Address, connection_id: int, duration: float
    ) -> None:
        """Publish a :class:`ConnectionCheckedOutEvent` to all connection
        listeners.
        """
        event = ConnectionCheckedOutEvent(address, connection_id, duration)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_checked_out(event)
            except Exception:
                _handle_exception()

    def publish_connection_checked_in(self, address: _Address, connection_id: int) -> None:
        """Publish a :class:`ConnectionCheckedInEvent` to all connection
        listeners.
        """
        event = ConnectionCheckedInEvent(address, connection_id)
        for subscriber in self.__cmap_listeners:
            try:
                subscriber.connection_checked_in(event)
            except Exception:
                _handle_exception()

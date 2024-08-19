# Copyright 2014-present MongoDB, Inc.
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

"""Internal class to monitor a topology of one or more servers."""

from __future__ import annotations

import logging
import os
import queue
import random
import sys
import time
import warnings
import weakref
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, cast

from pymongo import _csot, common, helpers, periodic_executor
from pymongo.client_session import _ServerSession, _ServerSessionPool
from pymongo.errors import (
    ConnectionFailure,
    InvalidOperation,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WriteError,
)
from pymongo.hello import Hello
from pymongo.lock import _create_lock
from pymongo.logger import (
    _SERVER_SELECTION_LOGGER,
    _debug_log,
    _ServerSelectionStatusMessage,
)
from pymongo.monitor import SrvMonitor
from pymongo.pool import Pool, PoolOptions
from pymongo.server import Server
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import (
    Selection,
    any_server_selector,
    arbiter_server_selector,
    secondary_server_selector,
    writable_server_selector,
)
from pymongo.topology_description import (
    SRV_POLLING_TOPOLOGIES,
    TOPOLOGY_TYPE,
    TopologyDescription,
    _updated_topology_description_srv_polling,
    updated_topology_description,
)

if TYPE_CHECKING:
    from bson import ObjectId
    from pymongo.settings import TopologySettings
    from pymongo.typings import ClusterTime, _Address


_pymongo_dir = str(Path(__file__).parent)


def process_events_queue(queue_ref: weakref.ReferenceType[queue.Queue]) -> bool:
    q = queue_ref()
    if not q:
        return False  # Cancel PeriodicExecutor.

    while True:
        try:
            event = q.get_nowait()
        except queue.Empty:
            break
        else:
            fn, args = event
            fn(*args)

    return True  # Continue PeriodicExecutor.


class Topology:
    """Monitor a topology of one or more servers."""

    def __init__(self, topology_settings: TopologySettings):
        self._topology_id = topology_settings._topology_id
        self._listeners = topology_settings._pool_options._event_listeners
        self._publish_server = self._listeners is not None and self._listeners.enabled_for_server
        self._publish_tp = self._listeners is not None and self._listeners.enabled_for_topology

        # Create events queue if there are publishers.
        self._events = None
        self.__events_executor: Any = None

        if self._publish_server or self._publish_tp:
            self._events = queue.Queue(maxsize=100)

        if self._publish_tp:
            assert self._events is not None
            self._events.put((self._listeners.publish_topology_opened, (self._topology_id,)))
        self._settings = topology_settings
        topology_description = TopologyDescription(
            topology_settings.get_topology_type(),
            topology_settings.get_server_descriptions(),
            topology_settings.replica_set_name,
            None,
            None,
            topology_settings,
        )

        self._description = topology_description
        if self._publish_tp:
            assert self._events is not None
            initial_td = TopologyDescription(
                TOPOLOGY_TYPE.Unknown, {}, None, None, None, self._settings
            )
            self._events.put(
                (
                    self._listeners.publish_topology_description_changed,
                    (initial_td, self._description, self._topology_id),
                )
            )

        for seed in topology_settings.seeds:
            if self._publish_server:
                assert self._events is not None
                self._events.put((self._listeners.publish_server_opened, (seed, self._topology_id)))

        # Store the seed list to help diagnose errors in _error_message().
        self._seed_addresses = list(topology_description.server_descriptions())
        self._opened = False
        self._closed = False
        self._lock = _create_lock()
        self._condition = self._settings.condition_class(self._lock)
        self._servers: dict[_Address, Server] = {}
        self._pid: Optional[int] = None
        self._max_cluster_time: Optional[ClusterTime] = None
        self._session_pool = _ServerSessionPool()

        if self._publish_server or self._publish_tp:
            assert self._events is not None
            weak: weakref.ReferenceType[queue.Queue]

            def target() -> bool:
                return process_events_queue(weak)

            executor = periodic_executor.PeriodicExecutor(
                interval=common.EVENTS_QUEUE_FREQUENCY,
                min_interval=common.MIN_HEARTBEAT_INTERVAL,
                target=target,
                name="pymongo_events_thread",
            )

            # We strongly reference the executor and it weakly references
            # the queue via this closure. When the topology is freed, stop
            # the executor soon.
            weak = weakref.ref(self._events, executor.close)
            self.__events_executor = executor
            executor.open()

        self._srv_monitor = None
        if self._settings.fqdn is not None and not self._settings.load_balanced:
            self._srv_monitor = SrvMonitor(self, self._settings)

    def open(self) -> None:
        """Start monitoring, or restart after a fork.

        No effect if called multiple times.

        .. warning:: Topology is shared among multiple threads and is protected
          by mutual exclusion. Using Topology from a process other than the one
          that initialized it will emit a warning and may result in deadlock. To
          prevent this from happening, MongoClient must be created after any
          forking.

        """
        pid = os.getpid()
        if self._pid is None:
            self._pid = pid
        elif pid != self._pid:
            self._pid = pid
            if sys.version_info[:2] >= (3, 12):
                kwargs = {"skip_file_prefixes": (_pymongo_dir,)}
            else:
                kwargs = {"stacklevel": 6}
            # Ignore B028 warning for missing stacklevel.
            warnings.warn(  # type: ignore[call-overload] # noqa: B028
                "MongoClient opened before fork. May not be entirely fork-safe, "
                "proceed with caution. See PyMongo's documentation for details: "
                "https://pymongo.readthedocs.io/en/stable/faq.html#"
                "is-pymongo-fork-safe",
                **kwargs,
            )
            with self._lock:
                # Close servers and clear the pools.
                for server in self._servers.values():
                    server.close()
                # Reset the session pool to avoid duplicate sessions in
                # the child process.
                self._session_pool.reset()

        with self._lock:
            self._ensure_opened()

    def get_server_selection_timeout(self) -> float:
        # CSOT: use remaining timeout when set.
        timeout = _csot.remaining()
        if timeout is None:
            return self._settings.server_selection_timeout
        return timeout

    def select_servers(
        self,
        selector: Callable[[Selection], Selection],
        operation: str,
        server_selection_timeout: Optional[float] = None,
        address: Optional[_Address] = None,
        operation_id: Optional[int] = None,
    ) -> list[Server]:
        """Return a list of Servers matching selector, or time out.

        :param selector: function that takes a list of Servers and returns
            a subset of them.
        :param operation: The name of the operation that the server is being selected for.
        :param server_selection_timeout: maximum seconds to wait.
            If not provided, the default value common.SERVER_SELECTION_TIMEOUT
            is used.
        :param address: optional server address to select.

        Calls self.open() if needed.

        Raises exc:`ServerSelectionTimeoutError` after
        `server_selection_timeout` if no matching servers are found.
        """
        if server_selection_timeout is None:
            server_timeout = self.get_server_selection_timeout()
        else:
            server_timeout = server_selection_timeout

        with self._lock:
            server_descriptions = self._select_servers_loop(
                selector, server_timeout, operation, operation_id, address
            )

            return [
                cast(Server, self.get_server_by_address(sd.address)) for sd in server_descriptions
            ]

    def _select_servers_loop(
        self,
        selector: Callable[[Selection], Selection],
        timeout: float,
        operation: str,
        operation_id: Optional[int],
        address: Optional[_Address],
    ) -> list[ServerDescription]:
        """select_servers() guts. Hold the lock when calling this."""
        now = time.monotonic()
        end_time = now + timeout
        logged_waiting = False

        if _SERVER_SELECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _SERVER_SELECTION_LOGGER,
                message=_ServerSelectionStatusMessage.STARTED,
                selector=selector,
                operation=operation,
                operationId=operation_id,
                topologyDescription=self.description,
                clientId=self.description._topology_settings._topology_id,
            )

        server_descriptions = self._description.apply_selector(
            selector, address, custom_selector=self._settings.server_selector
        )

        while not server_descriptions:
            # No suitable servers.
            if timeout == 0 or now > end_time:
                if _SERVER_SELECTION_LOGGER.isEnabledFor(logging.DEBUG):
                    _debug_log(
                        _SERVER_SELECTION_LOGGER,
                        message=_ServerSelectionStatusMessage.FAILED,
                        selector=selector,
                        operation=operation,
                        operationId=operation_id,
                        topologyDescription=self.description,
                        clientId=self.description._topology_settings._topology_id,
                        failure=self._error_message(selector),
                    )
                raise ServerSelectionTimeoutError(
                    f"{self._error_message(selector)}, Timeout: {timeout}s, Topology Description: {self.description!r}"
                )

            if not logged_waiting:
                _debug_log(
                    _SERVER_SELECTION_LOGGER,
                    message=_ServerSelectionStatusMessage.WAITING,
                    selector=selector,
                    operation=operation,
                    operationId=operation_id,
                    topologyDescription=self.description,
                    clientId=self.description._topology_settings._topology_id,
                    remainingTimeMS=int(end_time - time.monotonic()),
                )
                logged_waiting = True

            self._ensure_opened()
            self._request_check_all()

            # Release the lock and wait for the topology description to
            # change, or for a timeout. We won't miss any changes that
            # came after our most recent apply_selector call, since we've
            # held the lock until now.
            self._condition.wait(common.MIN_HEARTBEAT_INTERVAL)
            self._description.check_compatible()
            now = time.monotonic()
            server_descriptions = self._description.apply_selector(
                selector, address, custom_selector=self._settings.server_selector
            )

        self._description.check_compatible()
        return server_descriptions

    def _select_server(
        self,
        selector: Callable[[Selection], Selection],
        operation: str,
        server_selection_timeout: Optional[float] = None,
        address: Optional[_Address] = None,
        deprioritized_servers: Optional[list[Server]] = None,
        operation_id: Optional[int] = None,
    ) -> Server:
        servers = self.select_servers(
            selector, operation, server_selection_timeout, address, operation_id
        )
        servers = _filter_servers(servers, deprioritized_servers)
        if len(servers) == 1:
            return servers[0]
        server1, server2 = random.sample(servers, 2)
        if server1.pool.operation_count <= server2.pool.operation_count:
            return server1
        else:
            return server2

    def select_server(
        self,
        selector: Callable[[Selection], Selection],
        operation: str,
        server_selection_timeout: Optional[float] = None,
        address: Optional[_Address] = None,
        deprioritized_servers: Optional[list[Server]] = None,
        operation_id: Optional[int] = None,
    ) -> Server:
        """Like select_servers, but choose a random server if several match."""
        server = self._select_server(
            selector,
            operation,
            server_selection_timeout,
            address,
            deprioritized_servers,
            operation_id=operation_id,
        )
        if _csot.get_timeout():
            _csot.set_rtt(server.description.min_round_trip_time)
        if _SERVER_SELECTION_LOGGER.isEnabledFor(logging.DEBUG):
            _debug_log(
                _SERVER_SELECTION_LOGGER,
                message=_ServerSelectionStatusMessage.SUCCEEDED,
                selector=selector,
                operation=operation,
                operationId=operation_id,
                topologyDescription=self.description,
                clientId=self.description._topology_settings._topology_id,
                serverHost=server.description.address[0],
                serverPort=server.description.address[1],
            )
        return server

    def select_server_by_address(
        self,
        address: _Address,
        operation: str,
        server_selection_timeout: Optional[int] = None,
        operation_id: Optional[int] = None,
    ) -> Server:
        """Return a Server for "address", reconnecting if necessary.

        If the server's type is not known, request an immediate check of all
        servers. Time out after "server_selection_timeout" if the server
        cannot be reached.

        :param address: A (host, port) pair.
        :param operation: The name of the operation that the server is being selected for.
        :param server_selection_timeout: maximum seconds to wait.
            If not provided, the default value
            common.SERVER_SELECTION_TIMEOUT is used.
        :param operation_id: The unique id of the current operation being performed. Defaults to None if not provided.

        Calls self.open() if needed.

        Raises exc:`ServerSelectionTimeoutError` after
        `server_selection_timeout` if no matching servers are found.
        """
        return self.select_server(
            any_server_selector,
            operation,
            server_selection_timeout,
            address,
            operation_id=operation_id,
        )

    def _process_change(
        self,
        server_description: ServerDescription,
        reset_pool: bool = False,
        interrupt_connections: bool = False,
    ) -> None:
        """Process a new ServerDescription on an opened topology.

        Hold the lock when calling this.
        """
        td_old = self._description
        sd_old = td_old._server_descriptions[server_description.address]
        if _is_stale_server_description(sd_old, server_description):
            # This is a stale hello response. Ignore it.
            return

        new_td = updated_topology_description(self._description, server_description)
        # CMAP: Ensure the pool is "ready" when the server is selectable.
        if server_description.is_readable or (
            server_description.is_server_type_known and new_td.topology_type == TOPOLOGY_TYPE.Single
        ):
            server = self._servers.get(server_description.address)
            if server:
                server.pool.ready()

        suppress_event = (self._publish_server or self._publish_tp) and sd_old == server_description
        if self._publish_server and not suppress_event:
            assert self._events is not None
            self._events.put(
                (
                    self._listeners.publish_server_description_changed,
                    (sd_old, server_description, server_description.address, self._topology_id),
                )
            )

        self._description = new_td
        self._update_servers()
        self._receive_cluster_time_no_lock(server_description.cluster_time)

        if self._publish_tp and not suppress_event:
            assert self._events is not None
            self._events.put(
                (
                    self._listeners.publish_topology_description_changed,
                    (td_old, self._description, self._topology_id),
                )
            )

        # Shutdown SRV polling for unsupported cluster types.
        # This is only applicable if the old topology was Unknown, and the
        # new one is something other than Unknown or Sharded.
        if self._srv_monitor and (
            td_old.topology_type == TOPOLOGY_TYPE.Unknown
            and self._description.topology_type not in SRV_POLLING_TOPOLOGIES
        ):
            self._srv_monitor.close()

        # Clear the pool from a failed heartbeat.
        if reset_pool:
            server = self._servers.get(server_description.address)
            if server:
                server.pool.reset(interrupt_connections=interrupt_connections)

        # Wake waiters in select_servers().
        self._condition.notify_all()

    def on_change(
        self,
        server_description: ServerDescription,
        reset_pool: bool = False,
        interrupt_connections: bool = False,
    ) -> None:
        """Process a new ServerDescription after an hello call completes."""
        # We do no I/O holding the lock.
        with self._lock:
            # Monitors may continue working on hello calls for some time
            # after a call to Topology.close, so this method may be called at
            # any time. Ensure the topology is open before processing the
            # change.
            # Any monitored server was definitely in the topology description
            # once. Check if it's still in the description or if some state-
            # change removed it. E.g., we got a host list from the primary
            # that didn't include this server.
            if self._opened and self._description.has_server(server_description.address):
                self._process_change(server_description, reset_pool, interrupt_connections)

    def _process_srv_update(self, seedlist: list[tuple[str, Any]]) -> None:
        """Process a new seedlist on an opened topology.
        Hold the lock when calling this.
        """
        td_old = self._description
        if td_old.topology_type not in SRV_POLLING_TOPOLOGIES:
            return
        self._description = _updated_topology_description_srv_polling(self._description, seedlist)

        self._update_servers()

        if self._publish_tp:
            assert self._events is not None
            self._events.put(
                (
                    self._listeners.publish_topology_description_changed,
                    (td_old, self._description, self._topology_id),
                )
            )

    def on_srv_update(self, seedlist: list[tuple[str, Any]]) -> None:
        """Process a new list of nodes obtained from scanning SRV records."""
        # We do no I/O holding the lock.
        with self._lock:
            if self._opened:
                self._process_srv_update(seedlist)

    def get_server_by_address(self, address: _Address) -> Optional[Server]:
        """Get a Server or None.

        Returns the current version of the server immediately, even if it's
        Unknown or absent from the topology. Only use this in unittests.
        In driver code, use select_server_by_address, since then you're
        assured a recent view of the server's type and wire protocol version.
        """
        return self._servers.get(address)

    def has_server(self, address: _Address) -> bool:
        return address in self._servers

    def get_primary(self) -> Optional[_Address]:
        """Return primary's address or None."""
        # Implemented here in Topology instead of MongoClient, so it can lock.
        with self._lock:
            topology_type = self._description.topology_type
            if topology_type != TOPOLOGY_TYPE.ReplicaSetWithPrimary:
                return None

            return writable_server_selector(self._new_selection())[0].address

    def _get_replica_set_members(self, selector: Callable[[Selection], Selection]) -> set[_Address]:
        """Return set of replica set member addresses."""
        # Implemented here in Topology instead of MongoClient, so it can lock.
        with self._lock:
            topology_type = self._description.topology_type
            if topology_type not in (
                TOPOLOGY_TYPE.ReplicaSetWithPrimary,
                TOPOLOGY_TYPE.ReplicaSetNoPrimary,
            ):
                return set()

            return {sd.address for sd in iter(selector(self._new_selection()))}

    def get_secondaries(self) -> set[_Address]:
        """Return set of secondary addresses."""
        return self._get_replica_set_members(secondary_server_selector)

    def get_arbiters(self) -> set[_Address]:
        """Return set of arbiter addresses."""
        return self._get_replica_set_members(arbiter_server_selector)

    def max_cluster_time(self) -> Optional[ClusterTime]:
        """Return a document, the highest seen $clusterTime."""
        return self._max_cluster_time

    def _receive_cluster_time_no_lock(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        # Driver Sessions Spec: "Whenever a driver receives a cluster time from
        # a server it MUST compare it to the current highest seen cluster time
        # for the deployment. If the new cluster time is higher than the
        # highest seen cluster time it MUST become the new highest seen cluster
        # time. Two cluster times are compared using only the BsonTimestamp
        # value of the clusterTime embedded field."
        if cluster_time:
            # ">" uses bson.timestamp.Timestamp's comparison operator.
            if (
                not self._max_cluster_time
                or cluster_time["clusterTime"] > self._max_cluster_time["clusterTime"]
            ):
                self._max_cluster_time = cluster_time

    def receive_cluster_time(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        with self._lock:
            self._receive_cluster_time_no_lock(cluster_time)

    def request_check_all(self, wait_time: int = 5) -> None:
        """Wake all monitors, wait for at least one to check its server."""
        with self._lock:
            self._request_check_all()
            self._condition.wait(wait_time)

    def data_bearing_servers(self) -> list[ServerDescription]:
        """Return a list of all data-bearing servers.

        This includes any server that might be selected for an operation.
        """
        if self._description.topology_type == TOPOLOGY_TYPE.Single:
            return self._description.known_servers
        return self._description.readable_servers

    def update_pool(self) -> None:
        # Remove any stale sockets and add new sockets if pool is too small.
        servers = []
        with self._lock:
            # Only update pools for data-bearing servers.
            for sd in self.data_bearing_servers():
                server = self._servers[sd.address]
                servers.append((server, server.pool.gen.get_overall()))

        for server, generation in servers:
            try:
                server.pool.remove_stale_sockets(generation)
            except PyMongoError as exc:
                ctx = _ErrorContext(exc, 0, generation, False, None)
                self.handle_error(server.description.address, ctx)
                raise

    def close(self) -> None:
        """Clear pools and terminate monitors. Topology does not reopen on
        demand. Any further operations will raise
        :exc:`~.errors.InvalidOperation`.
        """
        with self._lock:
            for server in self._servers.values():
                server.close()

            # Mark all servers Unknown.
            self._description = self._description.reset()
            for address, sd in self._description.server_descriptions().items():
                if address in self._servers:
                    self._servers[address].description = sd

            # Stop SRV polling thread.
            if self._srv_monitor:
                self._srv_monitor.close()

            self._opened = False
            self._closed = True

        # Publish only after releasing the lock.
        if self._publish_tp:
            assert self._events is not None
            self._events.put((self._listeners.publish_topology_closed, (self._topology_id,)))
        if self._publish_server or self._publish_tp:
            self.__events_executor.close()

    @property
    def description(self) -> TopologyDescription:
        return self._description

    def pop_all_sessions(self) -> list[_ServerSession]:
        """Pop all session ids from the pool."""
        return self._session_pool.pop_all()

    def get_server_session(self, session_timeout_minutes: Optional[int]) -> _ServerSession:
        """Start or resume a server session, or raise ConfigurationError."""
        return self._session_pool.get_server_session(session_timeout_minutes)

    def return_server_session(self, server_session: _ServerSession) -> None:
        self._session_pool.return_server_session(server_session)

    def _new_selection(self) -> Selection:
        """A Selection object, initially including all known servers.

        Hold the lock when calling this.
        """
        return Selection.from_topology_description(self._description)

    def _ensure_opened(self) -> None:
        """Start monitors, or restart after a fork.

        Hold the lock when calling this.
        """
        if self._closed:
            raise InvalidOperation("Cannot use MongoClient after close")

        if not self._opened:
            self._opened = True
            self._update_servers()

            # Start or restart the events publishing thread.
            if self._publish_tp or self._publish_server:
                self.__events_executor.open()

            # Start the SRV polling thread.
            if self._srv_monitor and (self.description.topology_type in SRV_POLLING_TOPOLOGIES):
                self._srv_monitor.open()

            if self._settings.load_balanced:
                # Emit initial SDAM events for load balancer mode.
                self._process_change(
                    ServerDescription(
                        self._seed_addresses[0],
                        Hello({"ok": 1, "serviceId": self._topology_id, "maxWireVersion": 13}),
                    )
                )

        # Ensure that the monitors are open.
        for server in self._servers.values():
            server.open()

    def _is_stale_error(self, address: _Address, err_ctx: _ErrorContext) -> bool:
        server = self._servers.get(address)
        if server is None:
            # Another thread removed this server from the topology.
            return True

        if server._pool.stale_generation(err_ctx.sock_generation, err_ctx.service_id):
            # This is an outdated error from a previous pool version.
            return True

        # topologyVersion check, ignore error when cur_tv >= error_tv:
        cur_tv = server.description.topology_version
        error = err_ctx.error
        error_tv = None
        if error and hasattr(error, "details"):
            if isinstance(error.details, dict):
                error_tv = error.details.get("topologyVersion")

        return _is_stale_error_topology_version(cur_tv, error_tv)

    def _handle_error(self, address: _Address, err_ctx: _ErrorContext) -> None:
        if self._is_stale_error(address, err_ctx):
            return

        server = self._servers[address]
        error = err_ctx.error
        service_id = err_ctx.service_id

        # Ignore a handshake error if the server is behind a load balancer but
        # the service ID is unknown. This indicates that the error happened
        # when dialing the connection or during the MongoDB  handshake, so we
        # don't know the service ID to use for clearing the pool.
        if self._settings.load_balanced and not service_id and not err_ctx.completed_handshake:
            return

        if isinstance(error, NetworkTimeout) and err_ctx.completed_handshake:
            # The socket has been closed. Don't reset the server.
            # Server Discovery And Monitoring Spec: "When an application
            # operation fails because of any network error besides a socket
            # timeout...."
            return
        elif isinstance(error, WriteError):
            # Ignore writeErrors.
            return
        elif isinstance(error, (NotPrimaryError, OperationFailure)):
            # As per the SDAM spec if:
            #   - the server sees a "not primary" error, and
            #   - the server is not shutting down, and
            #   - the server version is >= 4.2, then
            # we keep the existing connection pool, but mark the server type
            # as Unknown and request an immediate check of the server.
            # Otherwise, we clear the connection pool, mark the server as
            # Unknown and request an immediate check of the server.
            if hasattr(error, "code"):
                err_code = error.code
            else:
                # Default error code if one does not exist.
                default = 10107 if isinstance(error, NotPrimaryError) else None
                err_code = error.details.get("code", default)  # type: ignore[union-attr]
            if err_code in helpers._NOT_PRIMARY_CODES:
                is_shutting_down = err_code in helpers._SHUTDOWN_CODES
                # Mark server Unknown, clear the pool, and request check.
                if not self._settings.load_balanced:
                    self._process_change(ServerDescription(address, error=error))
                if is_shutting_down or (err_ctx.max_wire_version <= 7):
                    # Clear the pool.
                    server.reset(service_id)
                server.request_check()
            elif not err_ctx.completed_handshake:
                # Unknown command error during the connection handshake.
                if not self._settings.load_balanced:
                    self._process_change(ServerDescription(address, error=error))
                # Clear the pool.
                server.reset(service_id)
        elif isinstance(error, ConnectionFailure):
            # "Client MUST replace the server's description with type Unknown
            # ... MUST NOT request an immediate check of the server."
            if not self._settings.load_balanced:
                self._process_change(ServerDescription(address, error=error))
            # Clear the pool.
            server.reset(service_id)
            # "When a client marks a server Unknown from `Network error when
            # reading or writing`_, clients MUST cancel the hello check on
            # that server and close the current monitoring connection."
            server._monitor.cancel_check()

    def handle_error(self, address: _Address, err_ctx: _ErrorContext) -> None:
        """Handle an application error.

        May reset the server to Unknown, clear the pool, and request an
        immediate check depending on the error and the context.
        """
        with self._lock:
            self._handle_error(address, err_ctx)

    def _request_check_all(self) -> None:
        """Wake all monitors. Hold the lock when calling this."""
        for server in self._servers.values():
            server.request_check()

    def _update_servers(self) -> None:
        """Sync our Servers from TopologyDescription.server_descriptions.

        Hold the lock while calling this.
        """
        for address, sd in self._description.server_descriptions().items():
            if address not in self._servers:
                monitor = self._settings.monitor_class(
                    server_description=sd,
                    topology=self,
                    pool=self._create_pool_for_monitor(address),
                    topology_settings=self._settings,
                )

                weak = None
                if self._publish_server and self._events is not None:
                    weak = weakref.ref(self._events)
                server = Server(
                    server_description=sd,
                    pool=self._create_pool_for_server(address),
                    monitor=monitor,
                    topology_id=self._topology_id,
                    listeners=self._listeners,
                    events=weak,
                )

                self._servers[address] = server
                server.open()
            else:
                # Cache old is_writable value.
                was_writable = self._servers[address].description.is_writable
                # Update server description.
                self._servers[address].description = sd
                # Update is_writable value of the pool, if it changed.
                if was_writable != sd.is_writable:
                    self._servers[address].pool.update_is_writable(sd.is_writable)

        for address, server in list(self._servers.items()):
            if not self._description.has_server(address):
                server.close()
                self._servers.pop(address)

    def _create_pool_for_server(self, address: _Address) -> Pool:
        return self._settings.pool_class(
            address, self._settings.pool_options, client_id=self._topology_id
        )

    def _create_pool_for_monitor(self, address: _Address) -> Pool:
        options = self._settings.pool_options

        # According to the Server Discovery And Monitoring Spec, monitors use
        # connect_timeout for both connect_timeout and socket_timeout. The
        # pool only has one socket so maxPoolSize and so on aren't needed.
        monitor_pool_options = PoolOptions(
            connect_timeout=options.connect_timeout,
            socket_timeout=options.connect_timeout,
            ssl_context=options._ssl_context,
            tls_allow_invalid_hostnames=options.tls_allow_invalid_hostnames,
            event_listeners=options._event_listeners,
            appname=options.appname,
            driver=options.driver,
            pause_enabled=False,
            server_api=options.server_api,
        )

        return self._settings.pool_class(
            address, monitor_pool_options, handshake=False, client_id=self._topology_id
        )

    def _error_message(self, selector: Callable[[Selection], Selection]) -> str:
        """Format an error message if server selection fails.

        Hold the lock when calling this.
        """
        is_replica_set = self._description.topology_type in (
            TOPOLOGY_TYPE.ReplicaSetWithPrimary,
            TOPOLOGY_TYPE.ReplicaSetNoPrimary,
        )

        if is_replica_set:
            server_plural = "replica set members"
        elif self._description.topology_type == TOPOLOGY_TYPE.Sharded:
            server_plural = "mongoses"
        else:
            server_plural = "servers"

        if self._description.known_servers:
            # We've connected, but no servers match the selector.
            if selector is writable_server_selector:
                if is_replica_set:
                    return "No primary available for writes"
                else:
                    return "No %s available for writes" % server_plural
            else:
                return f'No {server_plural} match selector "{selector}"'
        else:
            addresses = list(self._description.server_descriptions())
            servers = list(self._description.server_descriptions().values())
            if not servers:
                if is_replica_set:
                    # We removed all servers because of the wrong setName?
                    return 'No {} available for replica set name "{}"'.format(
                        server_plural,
                        self._settings.replica_set_name,
                    )
                else:
                    return "No %s available" % server_plural

            # 1 or more servers, all Unknown. Are they unknown for one reason?
            error = servers[0].error
            same = all(server.error == error for server in servers[1:])
            if same:
                if error is None:
                    # We're still discovering.
                    return "No %s found yet" % server_plural

                if is_replica_set and not set(addresses).intersection(self._seed_addresses):
                    # We replaced our seeds with new hosts but can't reach any.
                    return (
                        "Could not reach any servers in %s. Replica set is"
                        " configured with internal hostnames or IPs?" % addresses
                    )

                return str(error)
            else:
                return ",".join(str(server.error) for server in servers if server.error)

    def __repr__(self) -> str:
        msg = ""
        if not self._opened:
            msg = "CLOSED "
        return f"<{self.__class__.__name__} {msg}{self._description!r}>"

    def eq_props(self) -> tuple[tuple[_Address, ...], Optional[str], Optional[str], str]:
        """The properties to use for MongoClient/Topology equality checks."""
        ts = self._settings
        return (tuple(sorted(ts.seeds)), ts.replica_set_name, ts.fqdn, ts.srv_service_name)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.eq_props() == other.eq_props()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.eq_props())


class _ErrorContext:
    """An error with context for SDAM error handling."""

    def __init__(
        self,
        error: BaseException,
        max_wire_version: int,
        sock_generation: int,
        completed_handshake: bool,
        service_id: Optional[ObjectId],
    ):
        self.error = error
        self.max_wire_version = max_wire_version
        self.sock_generation = sock_generation
        self.completed_handshake = completed_handshake
        self.service_id = service_id


def _is_stale_error_topology_version(
    current_tv: Optional[Mapping[str, Any]], error_tv: Optional[Mapping[str, Any]]
) -> bool:
    """Return True if the error's topologyVersion is <= current."""
    if current_tv is None or error_tv is None:
        return False
    if current_tv["processId"] != error_tv["processId"]:
        return False
    return current_tv["counter"] >= error_tv["counter"]


def _is_stale_server_description(current_sd: ServerDescription, new_sd: ServerDescription) -> bool:
    """Return True if the new topologyVersion is < current."""
    current_tv, new_tv = current_sd.topology_version, new_sd.topology_version
    if current_tv is None or new_tv is None:
        return False
    if current_tv["processId"] != new_tv["processId"]:
        return False
    return current_tv["counter"] > new_tv["counter"]


def _filter_servers(
    candidates: list[Server], deprioritized_servers: Optional[list[Server]] = None
) -> list[Server]:
    """Filter out deprioritized servers from a list of server candidates."""
    if not deprioritized_servers:
        return candidates

    filtered = [server for server in candidates if server not in deprioritized_servers]

    # If not possible to pick a prioritized server, return the original list
    return filtered or candidates

# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Example event logger classes.

.. versionadded:: 3.11

These loggers can be registered using :func:`register` or
:class:`~pymongo.mongo_client.MongoClient`.

``monitoring.register(CommandLogger())``

or

``MongoClient(event_listeners=[CommandLogger()])``
"""
from __future__ import annotations

import logging

from pymongo import monitoring


class CommandLogger(monitoring.CommandListener):
    """A simple listener that logs command events.

    Listens for :class:`~pymongo.monitoring.CommandStartedEvent`,
    :class:`~pymongo.monitoring.CommandSucceededEvent` and
    :class:`~pymongo.monitoring.CommandFailedEvent` events and
    logs them at the `INFO` severity level using :mod:`logging`.
    .. versionadded:: 3.11
    """

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        logging.info(
            f"Command {event.command_name} with request id "
            f"{event.request_id} started on server "
            f"{event.connection_id}"
        )

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        logging.info(
            f"Command {event.command_name} with request id "
            f"{event.request_id} on server {event.connection_id} "
            f"succeeded in {event.duration_micros} "
            "microseconds"
        )

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        logging.info(
            f"Command {event.command_name} with request id "
            f"{event.request_id} on server {event.connection_id} "
            f"failed in {event.duration_micros} "
            "microseconds"
        )


class ServerLogger(monitoring.ServerListener):
    """A simple listener that logs server discovery events.

    Listens for :class:`~pymongo.monitoring.ServerOpeningEvent`,
    :class:`~pymongo.monitoring.ServerDescriptionChangedEvent`,
    and :class:`~pymongo.monitoring.ServerClosedEvent`
    events and logs them at the `INFO` severity level using :mod:`logging`.

    .. versionadded:: 3.11
    """

    def opened(self, event: monitoring.ServerOpeningEvent) -> None:
        logging.info(f"Server {event.server_address} added to topology {event.topology_id}")

    def description_changed(self, event: monitoring.ServerDescriptionChangedEvent) -> None:
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            # server_type_name was added in PyMongo 3.4
            logging.info(
                f"Server {event.server_address} changed type from "
                f"{event.previous_description.server_type_name} to "
                f"{event.new_description.server_type_name}"
            )

    def closed(self, event: monitoring.ServerClosedEvent) -> None:
        logging.warning(f"Server {event.server_address} removed from topology {event.topology_id}")


class HeartbeatLogger(monitoring.ServerHeartbeatListener):
    """A simple listener that logs server heartbeat events.

    Listens for :class:`~pymongo.monitoring.ServerHeartbeatStartedEvent`,
    :class:`~pymongo.monitoring.ServerHeartbeatSucceededEvent`,
    and :class:`~pymongo.monitoring.ServerHeartbeatFailedEvent`
    events and logs them at the `INFO` severity level using :mod:`logging`.

    .. versionadded:: 3.11
    """

    def started(self, event: monitoring.ServerHeartbeatStartedEvent) -> None:
        logging.info(f"Heartbeat sent to server {event.connection_id}")

    def succeeded(self, event: monitoring.ServerHeartbeatSucceededEvent) -> None:
        # The reply.document attribute was added in PyMongo 3.4.
        logging.info(
            f"Heartbeat to server {event.connection_id} "
            "succeeded with reply "
            f"{event.reply.document}"
        )

    def failed(self, event: monitoring.ServerHeartbeatFailedEvent) -> None:
        logging.warning(
            f"Heartbeat to server {event.connection_id} failed with error {event.reply}"
        )


class TopologyLogger(monitoring.TopologyListener):
    """A simple listener that logs server topology events.

    Listens for :class:`~pymongo.monitoring.TopologyOpenedEvent`,
    :class:`~pymongo.monitoring.TopologyDescriptionChangedEvent`,
    and :class:`~pymongo.monitoring.TopologyClosedEvent`
    events and logs them at the `INFO` severity level using :mod:`logging`.

    .. versionadded:: 3.11
    """

    def opened(self, event: monitoring.TopologyOpenedEvent) -> None:
        logging.info(f"Topology with id {event.topology_id} opened")

    def description_changed(self, event: monitoring.TopologyDescriptionChangedEvent) -> None:
        logging.info(f"Topology description updated for topology id {event.topology_id}")
        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            # topology_type_name was added in PyMongo 3.4
            logging.info(
                f"Topology {event.topology_id} changed type from "
                f"{event.previous_description.topology_type_name} to "
                f"{event.new_description.topology_type_name}"
            )
        # The has_writable_server and has_readable_server methods
        # were added in PyMongo 3.4.
        if not event.new_description.has_writable_server():
            logging.warning("No writable servers available.")
        if not event.new_description.has_readable_server():
            logging.warning("No readable servers available.")

    def closed(self, event: monitoring.TopologyClosedEvent) -> None:
        logging.info(f"Topology with id {event.topology_id} closed")


class ConnectionPoolLogger(monitoring.ConnectionPoolListener):
    """A simple listener that logs server connection pool events.

    Listens for :class:`~pymongo.monitoring.PoolCreatedEvent`,
    :class:`~pymongo.monitoring.PoolClearedEvent`,
    :class:`~pymongo.monitoring.PoolClosedEvent`,
    :~pymongo.monitoring.class:`ConnectionCreatedEvent`,
    :class:`~pymongo.monitoring.ConnectionReadyEvent`,
    :class:`~pymongo.monitoring.ConnectionClosedEvent`,
    :class:`~pymongo.monitoring.ConnectionCheckOutStartedEvent`,
    :class:`~pymongo.monitoring.ConnectionCheckOutFailedEvent`,
    :class:`~pymongo.monitoring.ConnectionCheckedOutEvent`,
    and :class:`~pymongo.monitoring.ConnectionCheckedInEvent`
    events and logs them at the `INFO` severity level using :mod:`logging`.

    .. versionadded:: 3.11
    """

    def pool_created(self, event: monitoring.PoolCreatedEvent) -> None:
        logging.info(f"[pool {event.address}] pool created")

    def pool_ready(self, event: monitoring.PoolReadyEvent) -> None:
        logging.info(f"[pool {event.address}] pool ready")

    def pool_cleared(self, event: monitoring.PoolClearedEvent) -> None:
        logging.info(f"[pool {event.address}] pool cleared")

    def pool_closed(self, event: monitoring.PoolClosedEvent) -> None:
        logging.info(f"[pool {event.address}] pool closed")

    def connection_created(self, event: monitoring.ConnectionCreatedEvent) -> None:
        logging.info(f"[pool {event.address}][conn #{event.connection_id}] connection created")

    def connection_ready(self, event: monitoring.ConnectionReadyEvent) -> None:
        logging.info(
            f"[pool {event.address}][conn #{event.connection_id}] connection setup succeeded"
        )

    def connection_closed(self, event: monitoring.ConnectionClosedEvent) -> None:
        logging.info(
            f"[pool {event.address}][conn #{event.connection_id}] "
            f'connection closed, reason: "{event.reason}"'
        )

    def connection_check_out_started(
        self, event: monitoring.ConnectionCheckOutStartedEvent
    ) -> None:
        logging.info(f"[pool {event.address}] connection check out started")

    def connection_check_out_failed(self, event: monitoring.ConnectionCheckOutFailedEvent) -> None:
        logging.info(f"[pool {event.address}] connection check out failed, reason: {event.reason}")

    def connection_checked_out(self, event: monitoring.ConnectionCheckedOutEvent) -> None:
        logging.info(
            f"[pool {event.address}][conn #{event.connection_id}] connection checked out of pool"
        )

    def connection_checked_in(self, event: monitoring.ConnectionCheckedInEvent) -> None:
        logging.info(
            f"[pool {event.address}][conn #{event.connection_id}] connection checked into pool"
        )

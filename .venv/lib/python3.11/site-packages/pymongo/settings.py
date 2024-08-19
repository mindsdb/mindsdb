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

"""Represent MongoClient's configuration."""
from __future__ import annotations

import threading
import traceback
from typing import Any, Collection, Optional, Type, Union

from bson.objectid import ObjectId
from pymongo import common, monitor, pool
from pymongo.common import LOCAL_THRESHOLD_MS, SERVER_SELECTION_TIMEOUT
from pymongo.errors import ConfigurationError
from pymongo.pool import Pool, PoolOptions
from pymongo.server_description import ServerDescription
from pymongo.topology_description import TOPOLOGY_TYPE, _ServerSelector


class TopologySettings:
    def __init__(
        self,
        seeds: Optional[Collection[tuple[str, int]]] = None,
        replica_set_name: Optional[str] = None,
        pool_class: Optional[Type[Pool]] = None,
        pool_options: Optional[PoolOptions] = None,
        monitor_class: Optional[Type[monitor.Monitor]] = None,
        condition_class: Optional[Type[threading.Condition]] = None,
        local_threshold_ms: int = LOCAL_THRESHOLD_MS,
        server_selection_timeout: int = SERVER_SELECTION_TIMEOUT,
        heartbeat_frequency: int = common.HEARTBEAT_FREQUENCY,
        server_selector: Optional[_ServerSelector] = None,
        fqdn: Optional[str] = None,
        direct_connection: Optional[bool] = False,
        load_balanced: Optional[bool] = None,
        srv_service_name: str = common.SRV_SERVICE_NAME,
        srv_max_hosts: int = 0,
        server_monitoring_mode: str = common.SERVER_MONITORING_MODE,
    ):
        """Represent MongoClient's configuration.

        Take a list of (host, port) pairs and optional replica set name.
        """
        if heartbeat_frequency < common.MIN_HEARTBEAT_INTERVAL:
            raise ConfigurationError(
                "heartbeatFrequencyMS cannot be less than %d"
                % (common.MIN_HEARTBEAT_INTERVAL * 1000,)
            )

        self._seeds: Collection[tuple[str, int]] = seeds or [("localhost", 27017)]
        self._replica_set_name = replica_set_name
        self._pool_class: Type[Pool] = pool_class or pool.Pool
        self._pool_options: PoolOptions = pool_options or PoolOptions()
        self._monitor_class: Type[monitor.Monitor] = monitor_class or monitor.Monitor
        self._condition_class: Type[threading.Condition] = condition_class or threading.Condition
        self._local_threshold_ms = local_threshold_ms
        self._server_selection_timeout = server_selection_timeout
        self._server_selector = server_selector
        self._fqdn = fqdn
        self._heartbeat_frequency = heartbeat_frequency
        self._direct = direct_connection
        self._load_balanced = load_balanced
        self._srv_service_name = srv_service_name
        self._srv_max_hosts = srv_max_hosts or 0
        self._server_monitoring_mode = server_monitoring_mode

        self._topology_id = ObjectId()
        # Store the allocation traceback to catch unclosed clients in the
        # test suite.
        self._stack = "".join(traceback.format_stack())

    @property
    def seeds(self) -> Collection[tuple[str, int]]:
        """List of server addresses."""
        return self._seeds

    @property
    def replica_set_name(self) -> Optional[str]:
        return self._replica_set_name

    @property
    def pool_class(self) -> Type[Pool]:
        return self._pool_class

    @property
    def pool_options(self) -> PoolOptions:
        return self._pool_options

    @property
    def monitor_class(self) -> Type[monitor.Monitor]:
        return self._monitor_class

    @property
    def condition_class(self) -> Type[threading.Condition]:
        return self._condition_class

    @property
    def local_threshold_ms(self) -> int:
        return self._local_threshold_ms

    @property
    def server_selection_timeout(self) -> int:
        return self._server_selection_timeout

    @property
    def server_selector(self) -> Optional[_ServerSelector]:
        return self._server_selector

    @property
    def heartbeat_frequency(self) -> int:
        return self._heartbeat_frequency

    @property
    def fqdn(self) -> Optional[str]:
        return self._fqdn

    @property
    def direct(self) -> Optional[bool]:
        """Connect directly to a single server, or use a set of servers?

        True if there is one seed and no replica_set_name.
        """
        return self._direct

    @property
    def load_balanced(self) -> Optional[bool]:
        """True if the client was configured to connect to a load balancer."""
        return self._load_balanced

    @property
    def srv_service_name(self) -> str:
        """The srvServiceName."""
        return self._srv_service_name

    @property
    def srv_max_hosts(self) -> int:
        """The srvMaxHosts."""
        return self._srv_max_hosts

    @property
    def server_monitoring_mode(self) -> str:
        """The serverMonitoringMode."""
        return self._server_monitoring_mode

    def get_topology_type(self) -> int:
        if self.load_balanced:
            return TOPOLOGY_TYPE.LoadBalanced
        elif self.direct:
            return TOPOLOGY_TYPE.Single
        elif self.replica_set_name is not None:
            return TOPOLOGY_TYPE.ReplicaSetNoPrimary
        else:
            return TOPOLOGY_TYPE.Unknown

    def get_server_descriptions(self) -> dict[Union[tuple[str, int], Any], ServerDescription]:
        """Initial dict of (address, ServerDescription) for all seeds."""
        return {address: ServerDescription(address) for address in self.seeds}

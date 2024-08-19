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

"""Represent a deployment of MongoDB servers."""
from __future__ import annotations

from random import sample
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    cast,
)

from bson.min_key import MinKey
from bson.objectid import ObjectId
from pymongo import common
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import ReadPreference, _AggWritePref, _ServerMode
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import Selection
from pymongo.server_type import SERVER_TYPE
from pymongo.typings import _Address


# Enumeration for various kinds of MongoDB cluster topologies.
class _TopologyType(NamedTuple):
    Single: int
    ReplicaSetNoPrimary: int
    ReplicaSetWithPrimary: int
    Sharded: int
    Unknown: int
    LoadBalanced: int


TOPOLOGY_TYPE = _TopologyType(*range(6))

# Topologies compatible with SRV record polling.
SRV_POLLING_TOPOLOGIES: tuple[int, int] = (TOPOLOGY_TYPE.Unknown, TOPOLOGY_TYPE.Sharded)


_ServerSelector = Callable[[List[ServerDescription]], List[ServerDescription]]


class TopologyDescription:
    def __init__(
        self,
        topology_type: int,
        server_descriptions: dict[_Address, ServerDescription],
        replica_set_name: Optional[str],
        max_set_version: Optional[int],
        max_election_id: Optional[ObjectId],
        topology_settings: Any,
    ) -> None:
        """Representation of a deployment of MongoDB servers.

        :param topology_type: initial type
        :param server_descriptions: dict of (address, ServerDescription) for
            all seeds
        :param replica_set_name: replica set name or None
        :param max_set_version: greatest setVersion seen from a primary, or None
        :param max_election_id: greatest electionId seen from a primary, or None
        :param topology_settings: a TopologySettings
        """
        self._topology_type = topology_type
        self._replica_set_name = replica_set_name
        self._server_descriptions = server_descriptions
        self._max_set_version = max_set_version
        self._max_election_id = max_election_id

        # The heartbeat_frequency is used in staleness estimates.
        self._topology_settings = topology_settings

        # Is PyMongo compatible with all servers' wire protocols?
        self._incompatible_err = None
        if self._topology_type != TOPOLOGY_TYPE.LoadBalanced:
            self._init_incompatible_err()

        # Server Discovery And Monitoring Spec: Whenever a client updates the
        # TopologyDescription from an hello response, it MUST set
        # TopologyDescription.logicalSessionTimeoutMinutes to the smallest
        # logicalSessionTimeoutMinutes value among ServerDescriptions of all
        # data-bearing server types. If any have a null
        # logicalSessionTimeoutMinutes, then
        # TopologyDescription.logicalSessionTimeoutMinutes MUST be set to null.
        readable_servers = self.readable_servers
        if not readable_servers:
            self._ls_timeout_minutes = None
        elif any(s.logical_session_timeout_minutes is None for s in readable_servers):
            self._ls_timeout_minutes = None
        else:
            self._ls_timeout_minutes = min(  # type: ignore[type-var]
                s.logical_session_timeout_minutes for s in readable_servers
            )

    def _init_incompatible_err(self) -> None:
        """Internal compatibility check for non-load balanced topologies."""
        for s in self._server_descriptions.values():
            if not s.is_server_type_known:
                continue

            # s.min/max_wire_version is the server's wire protocol.
            # MIN/MAX_SUPPORTED_WIRE_VERSION is what PyMongo supports.
            server_too_new = (
                # Server too new.
                s.min_wire_version is not None
                and s.min_wire_version > common.MAX_SUPPORTED_WIRE_VERSION
            )

            server_too_old = (
                # Server too old.
                s.max_wire_version is not None
                and s.max_wire_version < common.MIN_SUPPORTED_WIRE_VERSION
            )

            if server_too_new:
                self._incompatible_err = (
                    "Server at %s:%d requires wire version %d, but this "  # type: ignore
                    "version of PyMongo only supports up to %d."
                    % (
                        s.address[0],
                        s.address[1] or 0,
                        s.min_wire_version,
                        common.MAX_SUPPORTED_WIRE_VERSION,
                    )
                )

            elif server_too_old:
                self._incompatible_err = (
                    "Server at %s:%d reports wire version %d, but this "  # type: ignore
                    "version of PyMongo requires at least %d (MongoDB %s)."
                    % (
                        s.address[0],
                        s.address[1] or 0,
                        s.max_wire_version,
                        common.MIN_SUPPORTED_WIRE_VERSION,
                        common.MIN_SUPPORTED_SERVER_VERSION,
                    )
                )

                break

    def check_compatible(self) -> None:
        """Raise ConfigurationError if any server is incompatible.

        A server is incompatible if its wire protocol version range does not
        overlap with PyMongo's.
        """
        if self._incompatible_err:
            raise ConfigurationError(self._incompatible_err)

    def has_server(self, address: _Address) -> bool:
        return address in self._server_descriptions

    def reset_server(self, address: _Address) -> TopologyDescription:
        """A copy of this description, with one server marked Unknown."""
        unknown_sd = self._server_descriptions[address].to_unknown()
        return updated_topology_description(self, unknown_sd)

    def reset(self) -> TopologyDescription:
        """A copy of this description, with all servers marked Unknown."""
        if self._topology_type == TOPOLOGY_TYPE.ReplicaSetWithPrimary:
            topology_type = TOPOLOGY_TYPE.ReplicaSetNoPrimary
        else:
            topology_type = self._topology_type

        # The default ServerDescription's type is Unknown.
        sds = {address: ServerDescription(address) for address in self._server_descriptions}

        return TopologyDescription(
            topology_type,
            sds,
            self._replica_set_name,
            self._max_set_version,
            self._max_election_id,
            self._topology_settings,
        )

    def server_descriptions(self) -> dict[_Address, ServerDescription]:
        """dict of (address,
        :class:`~pymongo.server_description.ServerDescription`).
        """
        return self._server_descriptions.copy()

    @property
    def topology_type(self) -> int:
        """The type of this topology."""
        return self._topology_type

    @property
    def topology_type_name(self) -> str:
        """The topology type as a human readable string.

        .. versionadded:: 3.4
        """
        return TOPOLOGY_TYPE._fields[self._topology_type]

    @property
    def replica_set_name(self) -> Optional[str]:
        """The replica set name."""
        return self._replica_set_name

    @property
    def max_set_version(self) -> Optional[int]:
        """Greatest setVersion seen from a primary, or None."""
        return self._max_set_version

    @property
    def max_election_id(self) -> Optional[ObjectId]:
        """Greatest electionId seen from a primary, or None."""
        return self._max_election_id

    @property
    def logical_session_timeout_minutes(self) -> Optional[int]:
        """Minimum logical session timeout, or None."""
        return self._ls_timeout_minutes

    @property
    def known_servers(self) -> list[ServerDescription]:
        """List of Servers of types besides Unknown."""
        return [s for s in self._server_descriptions.values() if s.is_server_type_known]

    @property
    def has_known_servers(self) -> bool:
        """Whether there are any Servers of types besides Unknown."""
        return any(s for s in self._server_descriptions.values() if s.is_server_type_known)

    @property
    def readable_servers(self) -> list[ServerDescription]:
        """List of readable Servers."""
        return [s for s in self._server_descriptions.values() if s.is_readable]

    @property
    def common_wire_version(self) -> Optional[int]:
        """Minimum of all servers' max wire versions, or None."""
        servers = self.known_servers
        if servers:
            return min(s.max_wire_version for s in self.known_servers)

        return None

    @property
    def heartbeat_frequency(self) -> int:
        return self._topology_settings.heartbeat_frequency

    @property
    def srv_max_hosts(self) -> int:
        return self._topology_settings._srv_max_hosts

    def _apply_local_threshold(self, selection: Optional[Selection]) -> list[ServerDescription]:
        if not selection:
            return []
        round_trip_times: list[float] = []
        for server in selection.server_descriptions:
            if server.round_trip_time is None:
                config_err_msg = f"round_trip_time for server {server.address} is unexpectedly None: {self}, servers: {selection.server_descriptions}"
                raise ConfigurationError(config_err_msg)
            round_trip_times.append(server.round_trip_time)
        # Round trip time in seconds.
        fastest = min(round_trip_times)
        threshold = self._topology_settings.local_threshold_ms / 1000.0
        return [
            s
            for s in selection.server_descriptions
            if (cast(float, s.round_trip_time) - fastest) <= threshold
        ]

    def apply_selector(
        self,
        selector: Any,
        address: Optional[_Address] = None,
        custom_selector: Optional[_ServerSelector] = None,
    ) -> list[ServerDescription]:
        """List of servers matching the provided selector(s).

        :param selector: a callable that takes a Selection as input and returns
            a Selection as output. For example, an instance of a read
            preference from :mod:`~pymongo.read_preferences`.
        :param address: A server address to select.
        :param custom_selector: A callable that augments server
            selection rules. Accepts a list of
            :class:`~pymongo.server_description.ServerDescription` objects and
            return a list of server descriptions that should be considered
            suitable for the desired operation.

        .. versionadded:: 3.4
        """
        if getattr(selector, "min_wire_version", 0):
            common_wv = self.common_wire_version
            if common_wv and common_wv < selector.min_wire_version:
                raise ConfigurationError(
                    "%s requires min wire version %d, but topology's min"
                    " wire version is %d" % (selector, selector.min_wire_version, common_wv)
                )

        if isinstance(selector, _AggWritePref):
            selector.selection_hook(self)

        if self.topology_type == TOPOLOGY_TYPE.Unknown:
            return []
        elif self.topology_type in (TOPOLOGY_TYPE.Single, TOPOLOGY_TYPE.LoadBalanced):
            # Ignore selectors for standalone and load balancer mode.
            return self.known_servers
        if address:
            # Ignore selectors when explicit address is requested.
            description = self.server_descriptions().get(address)
            return [description] if description else []

        selection = Selection.from_topology_description(self)
        # Ignore read preference for sharded clusters.
        if self.topology_type != TOPOLOGY_TYPE.Sharded:
            selection = selector(selection)

        # Apply custom selector followed by localThresholdMS.
        if custom_selector is not None and selection:
            selection = selection.with_server_descriptions(
                custom_selector(selection.server_descriptions)
            )
        return self._apply_local_threshold(selection)

    def has_readable_server(self, read_preference: _ServerMode = ReadPreference.PRIMARY) -> bool:
        """Does this topology have any readable servers available matching the
        given read preference?

        :param read_preference: an instance of a read preference from
            :mod:`~pymongo.read_preferences`. Defaults to
            :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.

        .. note:: When connected directly to a single server this method
          always returns ``True``.

        .. versionadded:: 3.4
        """
        common.validate_read_preference("read_preference", read_preference)
        return any(self.apply_selector(read_preference))

    def has_writable_server(self) -> bool:
        """Does this topology have a writable server available?

        .. note:: When connected directly to a single server this method
          always returns ``True``.

        .. versionadded:: 3.4
        """
        return self.has_readable_server(ReadPreference.PRIMARY)

    def __repr__(self) -> str:
        # Sort the servers by address.
        servers = sorted(self._server_descriptions.values(), key=lambda sd: sd.address)
        return "<{} id: {}, topology_type: {}, servers: {!r}>".format(
            self.__class__.__name__,
            self._topology_settings._topology_id,
            self.topology_type_name,
            servers,
        )


# If topology type is Unknown and we receive a hello response, what should
# the new topology type be?
_SERVER_TYPE_TO_TOPOLOGY_TYPE = {
    SERVER_TYPE.Mongos: TOPOLOGY_TYPE.Sharded,
    SERVER_TYPE.RSPrimary: TOPOLOGY_TYPE.ReplicaSetWithPrimary,
    SERVER_TYPE.RSSecondary: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSArbiter: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    SERVER_TYPE.RSOther: TOPOLOGY_TYPE.ReplicaSetNoPrimary,
    # Note: SERVER_TYPE.LoadBalancer and Unknown are intentionally left out.
}


def updated_topology_description(
    topology_description: TopologyDescription, server_description: ServerDescription
) -> TopologyDescription:
    """Return an updated copy of a TopologyDescription.

    :param topology_description: the current TopologyDescription
    :param server_description: a new ServerDescription that resulted from
        a hello call

    Called after attempting (successfully or not) to call hello on the
    server at server_description.address. Does not modify topology_description.
    """
    address = server_description.address

    # These values will be updated, if necessary, to form the new
    # TopologyDescription.
    topology_type = topology_description.topology_type
    set_name = topology_description.replica_set_name
    max_set_version = topology_description.max_set_version
    max_election_id = topology_description.max_election_id
    server_type = server_description.server_type

    # Don't mutate the original dict of server descriptions; copy it.
    sds = topology_description.server_descriptions()

    # Replace this server's description with the new one.
    sds[address] = server_description

    if topology_type == TOPOLOGY_TYPE.Single:
        # Set server type to Unknown if replica set name does not match.
        if set_name is not None and set_name != server_description.replica_set_name:
            error = ConfigurationError(
                "client is configured to connect to a replica set named "
                "'{}' but this node belongs to a set named '{}'".format(
                    set_name, server_description.replica_set_name
                )
            )
            sds[address] = server_description.to_unknown(error=error)
        # Single type never changes.
        return TopologyDescription(
            TOPOLOGY_TYPE.Single,
            sds,
            set_name,
            max_set_version,
            max_election_id,
            topology_description._topology_settings,
        )

    if topology_type == TOPOLOGY_TYPE.Unknown:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.LoadBalancer):
            if len(topology_description._topology_settings.seeds) == 1:
                topology_type = TOPOLOGY_TYPE.Single
            else:
                # Remove standalone from Topology when given multiple seeds.
                sds.pop(address)
        elif server_type not in (SERVER_TYPE.Unknown, SERVER_TYPE.RSGhost):
            topology_type = _SERVER_TYPE_TO_TOPOLOGY_TYPE[server_type]

    if topology_type == TOPOLOGY_TYPE.Sharded:
        if server_type not in (SERVER_TYPE.Mongos, SERVER_TYPE.Unknown):
            sds.pop(address)

    elif topology_type == TOPOLOGY_TYPE.ReplicaSetNoPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)

        elif server_type == SERVER_TYPE.RSPrimary:
            (topology_type, set_name, max_set_version, max_election_id) = _update_rs_from_primary(
                sds, set_name, server_description, max_set_version, max_election_id
            )

        elif server_type in (SERVER_TYPE.RSSecondary, SERVER_TYPE.RSArbiter, SERVER_TYPE.RSOther):
            topology_type, set_name = _update_rs_no_primary_from_member(
                sds, set_name, server_description
            )

    elif topology_type == TOPOLOGY_TYPE.ReplicaSetWithPrimary:
        if server_type in (SERVER_TYPE.Standalone, SERVER_TYPE.Mongos):
            sds.pop(address)
            topology_type = _check_has_primary(sds)

        elif server_type == SERVER_TYPE.RSPrimary:
            (topology_type, set_name, max_set_version, max_election_id) = _update_rs_from_primary(
                sds, set_name, server_description, max_set_version, max_election_id
            )

        elif server_type in (SERVER_TYPE.RSSecondary, SERVER_TYPE.RSArbiter, SERVER_TYPE.RSOther):
            topology_type = _update_rs_with_primary_from_member(sds, set_name, server_description)

        else:
            # Server type is Unknown or RSGhost: did we just lose the primary?
            topology_type = _check_has_primary(sds)

    # Return updated copy.
    return TopologyDescription(
        topology_type,
        sds,
        set_name,
        max_set_version,
        max_election_id,
        topology_description._topology_settings,
    )


def _updated_topology_description_srv_polling(
    topology_description: TopologyDescription, seedlist: list[tuple[str, Any]]
) -> TopologyDescription:
    """Return an updated copy of a TopologyDescription.

    :param topology_description: the current TopologyDescription
    :param seedlist: a list of new seeds new ServerDescription that resulted from
        a hello call
    """
    assert topology_description.topology_type in SRV_POLLING_TOPOLOGIES
    # Create a copy of the server descriptions.
    sds = topology_description.server_descriptions()

    # If seeds haven't changed, don't do anything.
    if set(sds.keys()) == set(seedlist):
        return topology_description

    # Remove SDs corresponding to servers no longer part of the SRV record.
    for address in list(sds.keys()):
        if address not in seedlist:
            sds.pop(address)

    if topology_description.srv_max_hosts != 0:
        new_hosts = set(seedlist) - set(sds.keys())
        n_to_add = topology_description.srv_max_hosts - len(sds)
        if n_to_add > 0:
            seedlist = sample(sorted(new_hosts), min(n_to_add, len(new_hosts)))
        else:
            seedlist = []
    # Add SDs corresponding to servers recently added to the SRV record.
    for address in seedlist:
        if address not in sds:
            sds[address] = ServerDescription(address)
    return TopologyDescription(
        topology_description.topology_type,
        sds,
        topology_description.replica_set_name,
        topology_description.max_set_version,
        topology_description.max_election_id,
        topology_description._topology_settings,
    )


def _update_rs_from_primary(
    sds: MutableMapping[_Address, ServerDescription],
    replica_set_name: Optional[str],
    server_description: ServerDescription,
    max_set_version: Optional[int],
    max_election_id: Optional[ObjectId],
) -> tuple[int, Optional[str], Optional[int], Optional[ObjectId]]:
    """Update topology description from a primary's hello response.

    Pass in a dict of ServerDescriptions, current replica set name, the
    ServerDescription we are processing, and the TopologyDescription's
    max_set_version and max_election_id if any.

    Returns (new topology type, new replica_set_name, new max_set_version,
    new max_election_id).
    """
    if replica_set_name is None:
        replica_set_name = server_description.replica_set_name

    elif replica_set_name != server_description.replica_set_name:
        # We found a primary but it doesn't have the replica_set_name
        # provided by the user.
        sds.pop(server_description.address)
        return _check_has_primary(sds), replica_set_name, max_set_version, max_election_id

    if server_description.max_wire_version is None or server_description.max_wire_version < 17:
        new_election_tuple: tuple = (server_description.set_version, server_description.election_id)
        max_election_tuple: tuple = (max_set_version, max_election_id)
        if None not in new_election_tuple:
            if None not in max_election_tuple and new_election_tuple < max_election_tuple:
                # Stale primary, set to type Unknown.
                sds[server_description.address] = server_description.to_unknown()
                return _check_has_primary(sds), replica_set_name, max_set_version, max_election_id
            max_election_id = server_description.election_id

        if server_description.set_version is not None and (
            max_set_version is None or server_description.set_version > max_set_version
        ):
            max_set_version = server_description.set_version
    else:
        new_election_tuple = server_description.election_id, server_description.set_version
        max_election_tuple = max_election_id, max_set_version
        new_election_safe = tuple(MinKey() if i is None else i for i in new_election_tuple)
        max_election_safe = tuple(MinKey() if i is None else i for i in max_election_tuple)
        if new_election_safe < max_election_safe:
            # Stale primary, set to type Unknown.
            sds[server_description.address] = server_description.to_unknown()
            return _check_has_primary(sds), replica_set_name, max_set_version, max_election_id
        else:
            max_election_id = server_description.election_id
            max_set_version = server_description.set_version

    # We've heard from the primary. Is it the same primary as before?
    for server in sds.values():
        if (
            server.server_type is SERVER_TYPE.RSPrimary
            and server.address != server_description.address
        ):
            # Reset old primary's type to Unknown.
            sds[server.address] = server.to_unknown()

            # There can be only one prior primary.
            break

    # Discover new hosts from this primary's response.
    for new_address in server_description.all_hosts:
        if new_address not in sds:
            sds[new_address] = ServerDescription(new_address)

    # Remove hosts not in the response.
    for addr in set(sds) - server_description.all_hosts:
        sds.pop(addr)

    # If the host list differs from the seed list, we may not have a primary
    # after all.
    return (_check_has_primary(sds), replica_set_name, max_set_version, max_election_id)


def _update_rs_with_primary_from_member(
    sds: MutableMapping[_Address, ServerDescription],
    replica_set_name: Optional[str],
    server_description: ServerDescription,
) -> int:
    """RS with known primary. Process a response from a non-primary.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns new topology type.
    """
    assert replica_set_name is not None

    if replica_set_name != server_description.replica_set_name:
        sds.pop(server_description.address)
    elif server_description.me and server_description.address != server_description.me:
        sds.pop(server_description.address)

    # Had this member been the primary?
    return _check_has_primary(sds)


def _update_rs_no_primary_from_member(
    sds: MutableMapping[_Address, ServerDescription],
    replica_set_name: Optional[str],
    server_description: ServerDescription,
) -> tuple[int, Optional[str]]:
    """RS without known primary. Update from a non-primary's response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new topology type, new replica_set_name).
    """
    topology_type = TOPOLOGY_TYPE.ReplicaSetNoPrimary
    if replica_set_name is None:
        replica_set_name = server_description.replica_set_name

    elif replica_set_name != server_description.replica_set_name:
        sds.pop(server_description.address)
        return topology_type, replica_set_name

    # This isn't the primary's response, so don't remove any servers
    # it doesn't report. Only add new servers.
    for address in server_description.all_hosts:
        if address not in sds:
            sds[address] = ServerDescription(address)

    if server_description.me and server_description.address != server_description.me:
        sds.pop(server_description.address)

    return topology_type, replica_set_name


def _check_has_primary(sds: Mapping[_Address, ServerDescription]) -> int:
    """Current topology type is ReplicaSetWithPrimary. Is primary still known?

    Pass in a dict of ServerDescriptions.

    Returns new topology type.
    """
    for s in sds.values():
        if s.server_type == SERVER_TYPE.RSPrimary:
            return TOPOLOGY_TYPE.ReplicaSetWithPrimary
    else:  # noqa: PLW0120
        return TOPOLOGY_TYPE.ReplicaSetNoPrimary

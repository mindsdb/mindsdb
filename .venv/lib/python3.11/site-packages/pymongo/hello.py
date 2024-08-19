# Copyright 2021-present MongoDB, Inc.
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

"""Helpers for the 'hello' and legacy hello commands."""
from __future__ import annotations

import copy
import datetime
import itertools
from typing import Any, Generic, Mapping, Optional

from bson.objectid import ObjectId
from pymongo import common
from pymongo.server_type import SERVER_TYPE
from pymongo.typings import ClusterTime, _DocumentType


class HelloCompat:
    CMD = "hello"
    LEGACY_CMD = "ismaster"
    PRIMARY = "isWritablePrimary"
    LEGACY_PRIMARY = "ismaster"
    LEGACY_ERROR = "not master"


def _get_server_type(doc: Mapping[str, Any]) -> int:
    """Determine the server type from a hello response."""
    if not doc.get("ok"):
        return SERVER_TYPE.Unknown

    if doc.get("serviceId"):
        return SERVER_TYPE.LoadBalancer
    elif doc.get("isreplicaset"):
        return SERVER_TYPE.RSGhost
    elif doc.get("setName"):
        if doc.get("hidden"):
            return SERVER_TYPE.RSOther
        elif doc.get(HelloCompat.PRIMARY):
            return SERVER_TYPE.RSPrimary
        elif doc.get(HelloCompat.LEGACY_PRIMARY):
            return SERVER_TYPE.RSPrimary
        elif doc.get("secondary"):
            return SERVER_TYPE.RSSecondary
        elif doc.get("arbiterOnly"):
            return SERVER_TYPE.RSArbiter
        else:
            return SERVER_TYPE.RSOther
    elif doc.get("msg") == "isdbgrid":
        return SERVER_TYPE.Mongos
    else:
        return SERVER_TYPE.Standalone


class Hello(Generic[_DocumentType]):
    """Parse a hello response from the server.

    .. versionadded:: 3.12
    """

    __slots__ = ("_doc", "_server_type", "_is_writable", "_is_readable", "_awaitable")

    def __init__(self, doc: _DocumentType, awaitable: bool = False) -> None:
        self._server_type = _get_server_type(doc)
        self._doc: _DocumentType = doc
        self._is_writable = self._server_type in (
            SERVER_TYPE.RSPrimary,
            SERVER_TYPE.Standalone,
            SERVER_TYPE.Mongos,
            SERVER_TYPE.LoadBalancer,
        )

        self._is_readable = self.server_type == SERVER_TYPE.RSSecondary or self._is_writable
        self._awaitable = awaitable

    @property
    def document(self) -> _DocumentType:
        """The complete hello command response document.

        .. versionadded:: 3.4
        """
        return copy.copy(self._doc)

    @property
    def server_type(self) -> int:
        return self._server_type

    @property
    def all_hosts(self) -> set[tuple[str, int]]:
        """List of hosts, passives, and arbiters known to this server."""
        return set(
            map(
                common.clean_node,
                itertools.chain(
                    self._doc.get("hosts", []),
                    self._doc.get("passives", []),
                    self._doc.get("arbiters", []),
                ),
            )
        )

    @property
    def tags(self) -> Mapping[str, Any]:
        """Replica set member tags or empty dict."""
        return self._doc.get("tags", {})

    @property
    def primary(self) -> Optional[tuple[str, int]]:
        """This server's opinion about who the primary is, or None."""
        if self._doc.get("primary"):
            return common.partition_node(self._doc["primary"])
        else:
            return None

    @property
    def replica_set_name(self) -> Optional[str]:
        """Replica set name or None."""
        return self._doc.get("setName")

    @property
    def max_bson_size(self) -> int:
        return self._doc.get("maxBsonObjectSize", common.MAX_BSON_SIZE)

    @property
    def max_message_size(self) -> int:
        return self._doc.get("maxMessageSizeBytes", 2 * self.max_bson_size)

    @property
    def max_write_batch_size(self) -> int:
        return self._doc.get("maxWriteBatchSize", common.MAX_WRITE_BATCH_SIZE)

    @property
    def min_wire_version(self) -> int:
        return self._doc.get("minWireVersion", common.MIN_WIRE_VERSION)

    @property
    def max_wire_version(self) -> int:
        return self._doc.get("maxWireVersion", common.MAX_WIRE_VERSION)

    @property
    def set_version(self) -> Optional[int]:
        return self._doc.get("setVersion")

    @property
    def election_id(self) -> Optional[ObjectId]:
        return self._doc.get("electionId")

    @property
    def cluster_time(self) -> Optional[ClusterTime]:
        return self._doc.get("$clusterTime")

    @property
    def logical_session_timeout_minutes(self) -> Optional[int]:
        return self._doc.get("logicalSessionTimeoutMinutes")

    @property
    def is_writable(self) -> bool:
        return self._is_writable

    @property
    def is_readable(self) -> bool:
        return self._is_readable

    @property
    def me(self) -> Optional[tuple[str, int]]:
        me = self._doc.get("me")
        if me:
            return common.clean_node(me)
        return None

    @property
    def last_write_date(self) -> Optional[datetime.datetime]:
        return self._doc.get("lastWrite", {}).get("lastWriteDate")

    @property
    def compressors(self) -> Optional[list[str]]:
        return self._doc.get("compression")

    @property
    def sasl_supported_mechs(self) -> list[str]:
        """Supported authentication mechanisms for the current user.

        For example::

            >>> hello.sasl_supported_mechs
            ["SCRAM-SHA-1", "SCRAM-SHA-256"]

        """
        return self._doc.get("saslSupportedMechs", [])

    @property
    def speculative_authenticate(self) -> Optional[Mapping[str, Any]]:
        """The speculativeAuthenticate field."""
        return self._doc.get("speculativeAuthenticate")

    @property
    def topology_version(self) -> Optional[Mapping[str, Any]]:
        return self._doc.get("topologyVersion")

    @property
    def awaitable(self) -> bool:
        return self._awaitable

    @property
    def service_id(self) -> Optional[ObjectId]:
        return self._doc.get("serviceId")

    @property
    def hello_ok(self) -> bool:
        return self._doc.get("helloOk", False)

    @property
    def connection_id(self) -> Optional[int]:
        return self._doc.get("connectionId")

# Copyright 2012-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License",
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

"""Utilities for choosing which member of a replica set to read from."""

from __future__ import annotations

from collections import abc
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

from pymongo import max_staleness_selectors
from pymongo.errors import ConfigurationError
from pymongo.server_selectors import (
    member_with_tags_server_selector,
    secondary_with_tags_server_selector,
)

if TYPE_CHECKING:
    from pymongo.server_selectors import Selection
    from pymongo.topology_description import TopologyDescription

_PRIMARY = 0
_PRIMARY_PREFERRED = 1
_SECONDARY = 2
_SECONDARY_PREFERRED = 3
_NEAREST = 4


_MONGOS_MODES = (
    "primary",
    "primaryPreferred",
    "secondary",
    "secondaryPreferred",
    "nearest",
)

_Hedge = Mapping[str, Any]
_TagSets = Sequence[Mapping[str, Any]]


def _validate_tag_sets(tag_sets: Optional[_TagSets]) -> Optional[_TagSets]:
    """Validate tag sets for a MongoClient."""
    if tag_sets is None:
        return tag_sets

    if not isinstance(tag_sets, (list, tuple)):
        raise TypeError(f"Tag sets {tag_sets!r} invalid, must be a sequence")
    if len(tag_sets) == 0:
        raise ValueError(
            f"Tag sets {tag_sets!r} invalid, must be None or contain at least one set of tags"
        )

    for tags in tag_sets:
        if not isinstance(tags, abc.Mapping):
            raise TypeError(
                f"Tag set {tags!r} invalid, must be an instance of dict, "
                "bson.son.SON or other type that inherits from "
                "collection.Mapping"
            )

    return list(tag_sets)


def _invalid_max_staleness_msg(max_staleness: Any) -> str:
    return "maxStalenessSeconds must be a positive integer, not %s" % max_staleness


# Some duplication with common.py to avoid import cycle.
def _validate_max_staleness(max_staleness: Any) -> int:
    """Validate max_staleness."""
    if max_staleness == -1:
        return -1

    if not isinstance(max_staleness, int):
        raise TypeError(_invalid_max_staleness_msg(max_staleness))

    if max_staleness <= 0:
        raise ValueError(_invalid_max_staleness_msg(max_staleness))

    return max_staleness


def _validate_hedge(hedge: Optional[_Hedge]) -> Optional[_Hedge]:
    """Validate hedge."""
    if hedge is None:
        return None

    if not isinstance(hedge, dict):
        raise TypeError(f"hedge must be a dictionary, not {hedge!r}")

    return hedge


class _ServerMode:
    """Base class for all read preferences."""

    __slots__ = ("__mongos_mode", "__mode", "__tag_sets", "__max_staleness", "__hedge")

    def __init__(
        self,
        mode: int,
        tag_sets: Optional[_TagSets] = None,
        max_staleness: int = -1,
        hedge: Optional[_Hedge] = None,
    ) -> None:
        self.__mongos_mode = _MONGOS_MODES[mode]
        self.__mode = mode
        self.__tag_sets = _validate_tag_sets(tag_sets)
        self.__max_staleness = _validate_max_staleness(max_staleness)
        self.__hedge = _validate_hedge(hedge)

    @property
    def name(self) -> str:
        """The name of this read preference."""
        return self.__class__.__name__

    @property
    def mongos_mode(self) -> str:
        """The mongos mode of this read preference."""
        return self.__mongos_mode

    @property
    def document(self) -> dict[str, Any]:
        """Read preference as a document."""
        doc: dict[str, Any] = {"mode": self.__mongos_mode}
        if self.__tag_sets not in (None, [{}]):
            doc["tags"] = self.__tag_sets
        if self.__max_staleness != -1:
            doc["maxStalenessSeconds"] = self.__max_staleness
        if self.__hedge not in (None, {}):
            doc["hedge"] = self.__hedge
        return doc

    @property
    def mode(self) -> int:
        """The mode of this read preference instance."""
        return self.__mode

    @property
    def tag_sets(self) -> _TagSets:
        """Set ``tag_sets`` to a list of dictionaries like [{'dc': 'ny'}] to
        read only from members whose ``dc`` tag has the value ``"ny"``.
        To specify a priority-order for tag sets, provide a list of
        tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
        set, ``{}``, means "read from any member that matches the mode,
        ignoring tags." MongoClient tries each set of tags in turn
        until it finds a set of tags with at least one matching member.
        For example, to only send a query to an analytic node::

           Nearest(tag_sets=[{"node":"analytics"}])

        Or using :class:`SecondaryPreferred`::

           SecondaryPreferred(tag_sets=[{"node":"analytics"}])

           .. seealso:: `Data-Center Awareness
               <https://www.mongodb.com/docs/manual/data-center-awareness/>`_
        """
        return list(self.__tag_sets) if self.__tag_sets else [{}]

    @property
    def max_staleness(self) -> int:
        """The maximum estimated length of time (in seconds) a replica set
        secondary can fall behind the primary in replication before it will
        no longer be selected for operations, or -1 for no maximum.
        """
        return self.__max_staleness

    @property
    def hedge(self) -> Optional[_Hedge]:
        """The read preference ``hedge`` parameter.

        A dictionary that configures how the server will perform hedged reads.
        It consists of the following keys:

        - ``enabled``: Enables or disables hedged reads in sharded clusters.

        Hedged reads are automatically enabled in MongoDB 4.4+ when using a
        ``nearest`` read preference. To explicitly enable hedged reads, set
        the ``enabled`` key  to ``true``::

            >>> Nearest(hedge={'enabled': True})

        To explicitly disable hedged reads, set the ``enabled`` key  to
        ``False``::

            >>> Nearest(hedge={'enabled': False})

        .. versionadded:: 3.11
        """
        return self.__hedge

    @property
    def min_wire_version(self) -> int:
        """The wire protocol version the server must support.

        Some read preferences impose version requirements on all servers (e.g.
        maxStalenessSeconds requires MongoDB 3.4 / maxWireVersion 5).

        All servers' maxWireVersion must be at least this read preference's
        `min_wire_version`, or the driver raises
        :exc:`~pymongo.errors.ConfigurationError`.
        """
        return 0 if self.__max_staleness == -1 else 5

    def __repr__(self) -> str:
        return "{}(tag_sets={!r}, max_staleness={!r}, hedge={!r})".format(
            self.name,
            self.__tag_sets,
            self.__max_staleness,
            self.__hedge,
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _ServerMode):
            return (
                self.mode == other.mode
                and self.tag_sets == other.tag_sets
                and self.max_staleness == other.max_staleness
                and self.hedge == other.hedge
            )
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __getstate__(self) -> dict[str, Any]:
        """Return value of object for pickling.

        Needed explicitly because __slots__() defined.
        """
        return {
            "mode": self.__mode,
            "tag_sets": self.__tag_sets,
            "max_staleness": self.__max_staleness,
            "hedge": self.__hedge,
        }

    def __setstate__(self, value: Mapping[str, Any]) -> None:
        """Restore from pickling."""
        self.__mode = value["mode"]
        self.__mongos_mode = _MONGOS_MODES[self.__mode]
        self.__tag_sets = _validate_tag_sets(value["tag_sets"])
        self.__max_staleness = _validate_max_staleness(value["max_staleness"])
        self.__hedge = _validate_hedge(value["hedge"])

    def __call__(self, selection: Selection) -> Selection:
        return selection


class Primary(_ServerMode):
    """Primary read preference.

    * When directly connected to one mongod queries are allowed if the server
      is standalone or a replica set primary.
    * When connected to a mongos queries are sent to the primary of a shard.
    * When connected to a replica set queries are sent to the primary of
      the replica set.
    """

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(_PRIMARY)

    def __call__(self, selection: Selection) -> Selection:
        """Apply this read preference to a Selection."""
        return selection.primary_selection

    def __repr__(self) -> str:
        return "Primary()"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, _ServerMode):
            return other.mode == _PRIMARY
        return NotImplemented


class PrimaryPreferred(_ServerMode):
    """PrimaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are sent to the primary of a shard if
      available, otherwise a shard secondary.
    * When connected to a replica set queries are sent to the primary if
      available, otherwise a secondary.

    .. note:: When a :class:`~pymongo.mongo_client.MongoClient` is first
      created reads will be routed to an available secondary until the
      primary of the replica set is discovered.

    :param tag_sets: The :attr:`~tag_sets` to use if the primary is not
        available.
    :param max_staleness: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    :param hedge: The :attr:`~hedge` to use if the primary is not available.

    .. versionchanged:: 3.11
       Added ``hedge`` parameter.
    """

    __slots__ = ()

    def __init__(
        self,
        tag_sets: Optional[_TagSets] = None,
        max_staleness: int = -1,
        hedge: Optional[_Hedge] = None,
    ) -> None:
        super().__init__(_PRIMARY_PREFERRED, tag_sets, max_staleness, hedge)

    def __call__(self, selection: Selection) -> Selection:
        """Apply this read preference to Selection."""
        if selection.primary:
            return selection.primary_selection
        else:
            return secondary_with_tags_server_selector(
                self.tag_sets, max_staleness_selectors.select(self.max_staleness, selection)
            )


class Secondary(_ServerMode):
    """Secondary read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries. An error is raised if no secondaries are available.
    * When connected to a replica set queries are distributed among
      secondaries. An error is raised if no secondaries are available.

    :param tag_sets: The :attr:`~tag_sets` for this read preference.
    :param max_staleness: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    :param hedge: The :attr:`~hedge` for this read preference.

    .. versionchanged:: 3.11
       Added ``hedge`` parameter.
    """

    __slots__ = ()

    def __init__(
        self,
        tag_sets: Optional[_TagSets] = None,
        max_staleness: int = -1,
        hedge: Optional[_Hedge] = None,
    ) -> None:
        super().__init__(_SECONDARY, tag_sets, max_staleness, hedge)

    def __call__(self, selection: Selection) -> Selection:
        """Apply this read preference to Selection."""
        return secondary_with_tags_server_selector(
            self.tag_sets, max_staleness_selectors.select(self.max_staleness, selection)
        )


class SecondaryPreferred(_ServerMode):
    """SecondaryPreferred read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among shard
      secondaries, or the shard primary if no secondary is available.
    * When connected to a replica set queries are distributed among
      secondaries, or the primary if no secondary is available.

    .. note:: When a :class:`~pymongo.mongo_client.MongoClient` is first
      created reads will be routed to the primary of the replica set until
      an available secondary is discovered.

    :param tag_sets: The :attr:`~tag_sets` for this read preference.
    :param max_staleness: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    :param hedge: The :attr:`~hedge` for this read preference.

    .. versionchanged:: 3.11
       Added ``hedge`` parameter.
    """

    __slots__ = ()

    def __init__(
        self,
        tag_sets: Optional[_TagSets] = None,
        max_staleness: int = -1,
        hedge: Optional[_Hedge] = None,
    ) -> None:
        super().__init__(_SECONDARY_PREFERRED, tag_sets, max_staleness, hedge)

    def __call__(self, selection: Selection) -> Selection:
        """Apply this read preference to Selection."""
        secondaries = secondary_with_tags_server_selector(
            self.tag_sets, max_staleness_selectors.select(self.max_staleness, selection)
        )

        if secondaries:
            return secondaries
        else:
            return selection.primary_selection


class Nearest(_ServerMode):
    """Nearest read preference.

    * When directly connected to one mongod queries are allowed to standalone
      servers, to a replica set primary, or to replica set secondaries.
    * When connected to a mongos queries are distributed among all members of
      a shard.
    * When connected to a replica set queries are distributed among all
      members.

    :param tag_sets: The :attr:`~tag_sets` for this read preference.
    :param max_staleness: (integer, in seconds) The maximum estimated
        length of time a replica set secondary can fall behind the primary in
        replication before it will no longer be selected for operations.
        Default -1, meaning no maximum. If it is set, it must be at least
        90 seconds.
    :param hedge: The :attr:`~hedge` for this read preference.

    .. versionchanged:: 3.11
       Added ``hedge`` parameter.
    """

    __slots__ = ()

    def __init__(
        self,
        tag_sets: Optional[_TagSets] = None,
        max_staleness: int = -1,
        hedge: Optional[_Hedge] = None,
    ) -> None:
        super().__init__(_NEAREST, tag_sets, max_staleness, hedge)

    def __call__(self, selection: Selection) -> Selection:
        """Apply this read preference to Selection."""
        return member_with_tags_server_selector(
            self.tag_sets, max_staleness_selectors.select(self.max_staleness, selection)
        )


class _AggWritePref:
    """Agg $out/$merge write preference.

    * If there are readable servers and there is any pre-5.0 server, use
      primary read preference.
    * Otherwise use `pref` read preference.

    :param pref: The read preference to use on MongoDB 5.0+.
    """

    __slots__ = ("pref", "effective_pref")

    def __init__(self, pref: _ServerMode):
        self.pref = pref
        self.effective_pref: _ServerMode = ReadPreference.PRIMARY

    def selection_hook(self, topology_description: TopologyDescription) -> None:
        common_wv = topology_description.common_wire_version
        if (
            topology_description.has_readable_server(ReadPreference.PRIMARY_PREFERRED)
            and common_wv
            and common_wv < 13
        ):
            self.effective_pref = ReadPreference.PRIMARY
        else:
            self.effective_pref = self.pref

    def __call__(self, selection: Selection) -> Selection:
        """Apply this read preference to a Selection."""
        return self.effective_pref(selection)

    def __repr__(self) -> str:
        return f"_AggWritePref(pref={self.pref!r})"

    # Proxy other calls to the effective_pref so that _AggWritePref can be
    # used in place of an actual read preference.
    def __getattr__(self, name: str) -> Any:
        return getattr(self.effective_pref, name)


_ALL_READ_PREFERENCES = (Primary, PrimaryPreferred, Secondary, SecondaryPreferred, Nearest)


def make_read_preference(
    mode: int, tag_sets: Optional[_TagSets], max_staleness: int = -1
) -> _ServerMode:
    if mode == _PRIMARY:
        if tag_sets not in (None, [{}]):
            raise ConfigurationError("Read preference primary cannot be combined with tags")
        if max_staleness != -1:
            raise ConfigurationError(
                "Read preference primary cannot be combined with maxStalenessSeconds"
            )
        return Primary()
    return _ALL_READ_PREFERENCES[mode](tag_sets, max_staleness)  # type: ignore


_MODES = (
    "PRIMARY",
    "PRIMARY_PREFERRED",
    "SECONDARY",
    "SECONDARY_PREFERRED",
    "NEAREST",
)


class ReadPreference:
    """An enum that defines some commonly used read preference modes.

    Apps can also create a custom read preference, for example::

       Nearest(tag_sets=[{"node":"analytics"}])

    See :doc:`/examples/high_availability` for code examples.

    A read preference is used in three cases:

    :class:`~pymongo.mongo_client.MongoClient` connected to a single mongod:

    - ``PRIMARY``: Queries are allowed if the server is standalone or a replica
      set primary.
    - All other modes allow queries to standalone servers, to a replica set
      primary, or to replica set secondaries.

    :class:`~pymongo.mongo_client.MongoClient` initialized with the
    ``replicaSet`` option:

    - ``PRIMARY``: Read from the primary. This is the default, and provides the
      strongest consistency. If no primary is available, raise
      :class:`~pymongo.errors.AutoReconnect`.

    - ``PRIMARY_PREFERRED``: Read from the primary if available, or if there is
      none, read from a secondary.

    - ``SECONDARY``: Read from a secondary. If no secondary is available,
      raise :class:`~pymongo.errors.AutoReconnect`.

    - ``SECONDARY_PREFERRED``: Read from a secondary if available, otherwise
      from the primary.

    - ``NEAREST``: Read from any member.

    :class:`~pymongo.mongo_client.MongoClient` connected to a mongos, with a
    sharded cluster of replica sets:

    - ``PRIMARY``: Read from the primary of the shard, or raise
      :class:`~pymongo.errors.OperationFailure` if there is none.
      This is the default.

    - ``PRIMARY_PREFERRED``: Read from the primary of the shard, or if there is
      none, read from a secondary of the shard.

    - ``SECONDARY``: Read from a secondary of the shard, or raise
      :class:`~pymongo.errors.OperationFailure` if there is none.

    - ``SECONDARY_PREFERRED``: Read from a secondary of the shard if available,
      otherwise from the shard primary.

    - ``NEAREST``: Read from any shard member.
    """

    PRIMARY = Primary()
    PRIMARY_PREFERRED = PrimaryPreferred()
    SECONDARY = Secondary()
    SECONDARY_PREFERRED = SecondaryPreferred()
    NEAREST = Nearest()


def read_pref_mode_from_name(name: str) -> int:
    """Get the read preference mode from mongos/uri name."""
    return _MONGOS_MODES.index(name)


class MovingAverage:
    """Tracks an exponentially-weighted moving average."""

    average: Optional[float]

    def __init__(self) -> None:
        self.average = None

    def add_sample(self, sample: float) -> None:
        if sample < 0:
            # Likely system time change while waiting for hello response
            # and not using time.monotonic. Ignore it, the next one will
            # probably be valid.
            return
        if self.average is None:
            self.average = sample
        else:
            # The Server Selection Spec requires an exponentially weighted
            # average with alpha = 0.2.
            self.average = 0.8 * self.average + 0.2 * sample

    def get(self) -> Optional[float]:
        """Get the calculated average, or None if no samples yet."""
        return self.average

    def reset(self) -> None:
        self.average = None

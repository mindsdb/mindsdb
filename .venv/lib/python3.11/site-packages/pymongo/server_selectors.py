# Copyright 2014-2016 MongoDB, Inc.
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

"""Criteria to select some ServerDescriptions from a TopologyDescription."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, TypeVar, cast

from pymongo.server_type import SERVER_TYPE

if TYPE_CHECKING:
    from pymongo.server_description import ServerDescription
    from pymongo.topology_description import TopologyDescription


T = TypeVar("T")
TagSet = Mapping[str, Any]
TagSets = Sequence[TagSet]


class Selection:
    """Input or output of a server selector function."""

    @classmethod
    def from_topology_description(cls, topology_description: TopologyDescription) -> Selection:
        known_servers = topology_description.known_servers
        primary = None
        for sd in known_servers:
            if sd.server_type == SERVER_TYPE.RSPrimary:
                primary = sd
                break

        return Selection(
            topology_description,
            topology_description.known_servers,
            topology_description.common_wire_version,
            primary,
        )

    def __init__(
        self,
        topology_description: TopologyDescription,
        server_descriptions: list[ServerDescription],
        common_wire_version: Optional[int],
        primary: Optional[ServerDescription],
    ):
        self.topology_description = topology_description
        self.server_descriptions = server_descriptions
        self.primary = primary
        self.common_wire_version = common_wire_version

    def with_server_descriptions(self, server_descriptions: list[ServerDescription]) -> Selection:
        return Selection(
            self.topology_description, server_descriptions, self.common_wire_version, self.primary
        )

    def secondary_with_max_last_write_date(self) -> Optional[ServerDescription]:
        secondaries = secondary_server_selector(self)
        if secondaries.server_descriptions:
            return max(
                secondaries.server_descriptions, key=lambda sd: cast(float, sd.last_write_date)
            )
        return None

    @property
    def primary_selection(self) -> Selection:
        primaries = [self.primary] if self.primary else []
        return self.with_server_descriptions(primaries)

    @property
    def heartbeat_frequency(self) -> int:
        return self.topology_description.heartbeat_frequency

    @property
    def topology_type(self) -> int:
        return self.topology_description.topology_type

    def __bool__(self) -> bool:
        return bool(self.server_descriptions)

    def __getitem__(self, item: int) -> ServerDescription:
        return self.server_descriptions[item]


def any_server_selector(selection: T) -> T:
    return selection


def readable_server_selector(selection: Selection) -> Selection:
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if s.is_readable]
    )


def writable_server_selector(selection: Selection) -> Selection:
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if s.is_writable]
    )


def secondary_server_selector(selection: Selection) -> Selection:
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if s.server_type == SERVER_TYPE.RSSecondary]
    )


def arbiter_server_selector(selection: Selection) -> Selection:
    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if s.server_type == SERVER_TYPE.RSArbiter]
    )


def writable_preferred_server_selector(selection: Selection) -> Selection:
    """Like PrimaryPreferred but doesn't use tags or latency."""
    return writable_server_selector(selection) or secondary_server_selector(selection)


def apply_single_tag_set(tag_set: TagSet, selection: Selection) -> Selection:
    """All servers matching one tag set.

    A tag set is a dict. A server matches if its tags are a superset:
    A server tagged {'a': '1', 'b': '2'} matches the tag set {'a': '1'}.

    The empty tag set {} matches any server.
    """

    def tags_match(server_tags: Mapping[str, Any]) -> bool:
        for key, value in tag_set.items():
            if key not in server_tags or server_tags[key] != value:
                return False

        return True

    return selection.with_server_descriptions(
        [s for s in selection.server_descriptions if tags_match(s.tags)]
    )


def apply_tag_sets(tag_sets: TagSets, selection: Selection) -> Selection:
    """All servers match a list of tag sets.

    tag_sets is a list of dicts. The empty tag set {} matches any server,
    and may be provided at the end of the list as a fallback. So
    [{'a': 'value'}, {}] expresses a preference for servers tagged
    {'a': 'value'}, but accepts any server if none matches the first
    preference.
    """
    for tag_set in tag_sets:
        with_tag_set = apply_single_tag_set(tag_set, selection)
        if with_tag_set:
            return with_tag_set

    return selection.with_server_descriptions([])


def secondary_with_tags_server_selector(tag_sets: TagSets, selection: Selection) -> Selection:
    """All near-enough secondaries matching the tag sets."""
    return apply_tag_sets(tag_sets, secondary_server_selector(selection))


def member_with_tags_server_selector(tag_sets: TagSets, selection: Selection) -> Selection:
    """All near-enough members matching the tag sets."""
    return apply_tag_sets(tag_sets, readable_server_selector(selection))

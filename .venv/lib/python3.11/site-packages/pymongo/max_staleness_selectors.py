# Copyright 2016 MongoDB, Inc.
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

"""Criteria to select ServerDescriptions based on maxStalenessSeconds.

The Max Staleness Spec says: When there is a known primary P,
a secondary S's staleness is estimated with this formula:

  (S.lastUpdateTime - S.lastWriteDate) - (P.lastUpdateTime - P.lastWriteDate)
  + heartbeatFrequencyMS

When there is no known primary, a secondary S's staleness is estimated with:

  SMax.lastWriteDate - S.lastWriteDate + heartbeatFrequencyMS

where "SMax" is the secondary with the greatest lastWriteDate.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pymongo.errors import ConfigurationError
from pymongo.server_type import SERVER_TYPE

if TYPE_CHECKING:
    from pymongo.server_selectors import Selection
# Constant defined in Max Staleness Spec: An idle primary writes a no-op every
# 10 seconds to refresh secondaries' lastWriteDate values.
IDLE_WRITE_PERIOD = 10
SMALLEST_MAX_STALENESS = 90


def _validate_max_staleness(max_staleness: int, heartbeat_frequency: int) -> None:
    # We checked for max staleness -1 before this, it must be positive here.
    if max_staleness < heartbeat_frequency + IDLE_WRITE_PERIOD:
        raise ConfigurationError(
            "maxStalenessSeconds must be at least heartbeatFrequencyMS +"
            " %d seconds. maxStalenessSeconds is set to %d,"
            " heartbeatFrequencyMS is set to %d."
            % (IDLE_WRITE_PERIOD, max_staleness, heartbeat_frequency * 1000)
        )

    if max_staleness < SMALLEST_MAX_STALENESS:
        raise ConfigurationError(
            "maxStalenessSeconds must be at least %d. "
            "maxStalenessSeconds is set to %d." % (SMALLEST_MAX_STALENESS, max_staleness)
        )


def _with_primary(max_staleness: int, selection: Selection) -> Selection:
    """Apply max_staleness, in seconds, to a Selection with a known primary."""
    primary = selection.primary
    assert primary
    sds = []

    for s in selection.server_descriptions:
        if s.server_type == SERVER_TYPE.RSSecondary:
            # See max-staleness.rst for explanation of this formula.
            assert s.last_write_date and primary.last_write_date  # noqa: PT018
            staleness = (
                (s.last_update_time - s.last_write_date)
                - (primary.last_update_time - primary.last_write_date)
                + selection.heartbeat_frequency
            )

            if staleness <= max_staleness:
                sds.append(s)
        else:
            sds.append(s)

    return selection.with_server_descriptions(sds)


def _no_primary(max_staleness: int, selection: Selection) -> Selection:
    """Apply max_staleness, in seconds, to a Selection with no known primary."""
    # Secondary that's replicated the most recent writes.
    smax = selection.secondary_with_max_last_write_date()
    if not smax:
        # No secondaries and no primary, short-circuit out of here.
        return selection.with_server_descriptions([])

    sds = []

    for s in selection.server_descriptions:
        if s.server_type == SERVER_TYPE.RSSecondary:
            # See max-staleness.rst for explanation of this formula.
            assert smax.last_write_date and s.last_write_date  # noqa: PT018
            staleness = smax.last_write_date - s.last_write_date + selection.heartbeat_frequency

            if staleness <= max_staleness:
                sds.append(s)
        else:
            sds.append(s)

    return selection.with_server_descriptions(sds)


def select(max_staleness: int, selection: Selection) -> Selection:
    """Apply max_staleness, in seconds, to a Selection."""
    if max_staleness == -1:
        return selection

    # Server Selection Spec: If the TopologyType is ReplicaSetWithPrimary or
    # ReplicaSetNoPrimary, a client MUST raise an error if maxStaleness <
    # heartbeatFrequency + IDLE_WRITE_PERIOD, or if maxStaleness < 90.
    _validate_max_staleness(max_staleness, selection.heartbeat_frequency)

    if selection.primary:
        return _with_primary(max_staleness, selection)
    else:
        return _no_primary(max_staleness, selection)
